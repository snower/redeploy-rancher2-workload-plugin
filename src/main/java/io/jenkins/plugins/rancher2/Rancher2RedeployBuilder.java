package io.jenkins.plugins.rancher2;

import com.cloudbees.plugins.credentials.CredentialsMatchers;
import com.cloudbees.plugins.credentials.CredentialsProvider;
import com.cloudbees.plugins.credentials.common.StandardListBoxModel;
import com.cloudbees.plugins.credentials.domains.DomainRequirement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import hudson.*;
import hudson.model.Item;
import hudson.security.ACL;
import hudson.util.FormValidation;
import hudson.model.AbstractProject;
import hudson.model.Run;
import hudson.model.TaskListener;
import hudson.tasks.Builder;
import hudson.tasks.BuildStepDescriptor;
import hudson.util.ListBoxModel;
import jenkins.model.Jenkins;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.kohsuke.stapler.AncestorInPath;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.QueryParameter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import jenkins.tasks.SimpleBuildStep;
import org.jenkinsci.Symbol;

public class Rancher2RedeployBuilder extends Builder implements SimpleBuildStep {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Nonnull
    private String credential;
    @Nonnull
    private final String workload;
    private final String images;
    private final boolean alwaysPull;
    private final boolean pollingDeployFinish;
    private final int pollingDeployTimeout;

    @DataBoundConstructor
    public Rancher2RedeployBuilder(
            @Nonnull String credential,
            @Nonnull String workload,
            @Nullable String images,
            boolean alwaysPull,
            boolean pollingDeployFinish,
            int pollingDeployTimeout
    ) {
        this.credential = credential;
        this.workload = workload;
        this.images = images;
        this.alwaysPull = alwaysPull;
        this.pollingDeployFinish = pollingDeployFinish;
        this.pollingDeployTimeout = pollingDeployTimeout;
    }

    @Nonnull
    public String getCredential() {
        return credential;
    }

    @Nonnull
    public String getWorkload() {
        return workload;
    }

    public String getImages() {
        return images;
    }

    public boolean isAlwaysPull() {
        return alwaysPull;
    }

    public boolean isPollingDeployFinish() {
        return pollingDeployFinish;
    }

    public int getPollingDeployTimeout() {
        return pollingDeployTimeout;
    }

    private Set<String> getWorkloadPods(PrintStream logger, CloseableHttpClient client, Rancher2Credentials credential, String url) throws InterruptedException, IOException {
        String[] urlInfo = url.split("/workloads/");
        if(urlInfo.length != 2) {
            throw new AbortException(Messages.Rancher2RedeployBuilder_badWorkload(url));
        }

        HttpUriRequest request = RequestBuilder.get(urlInfo[0] + "/pods?workloadId=" + URLEncoder.encode(urlInfo[1], "UTF-8"))
                .addHeader("Authorization", "Bearer " + credential.getBearerToken())
                .addHeader("Accept", "application/json")
                .build();

        CloseableHttpResponse response = client.execute(request);
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new AbortException(
                    Messages.Rancher2RedeployBuilder_badResponse(
                            response.getStatusLine().getStatusCode(),
                            EntityUtils.toString(response.getEntity())
                    )
            );
        }

        Set<String> workloadPods = new HashSet<>();
        JsonNode root = MAPPER.readTree(response.getEntity().getContent());
        JsonNode pods = root.get("data");
        for (int i = 0; pods != null && i < pods.size(); i++) {
            String podID = pods.get(i).get("id").asText();
            if (podID != null) {
                workloadPods.add(podID);
            }
        }
        logger.println(Messages._Rancher2RedeployBuilder_loadWorkloadPodsSuccess(workloadPods.stream().collect(Collectors.joining(","))));
        return workloadPods;
    }

    private void pollingCheckPodsDeployFinish(PrintStream logger, CloseableHttpClient client, Rancher2Credentials credential, String url, Set<String> lastDeployPods) throws InterruptedException, IOException {
        long startTime = (new Date()).getTime();
        Thread.sleep(5000);
        while (((new Date()).getTime() - startTime) < ((long) pollingDeployTimeout) * 1000) {
            Set<String> deployPods = getWorkloadPods(logger, client, credential, url);
            deployPods.retainAll(lastDeployPods);
            if(deployPods.size() <= 0) {
                return;
            }

            Thread.sleep(3000);
        }
        throw new AbortException(Messages.Rancher2RedeployBuilder_pollingDeployTimeout((int)(((new Date()).getTime() - startTime)/1000)));
    }

    @Override
    public void perform(Run<?, ?> run, FilePath workspace, Launcher launcher, TaskListener listener) throws InterruptedException, IOException {
        PrintStream logger = listener.getLogger();
        EnvVars envVars = run.getEnvironment(listener);
        String credentialId = envVars.expand(this.credential);

        Rancher2Credentials credential = CredentialsProvider.findCredentialById(
                credentialId,
                Rancher2Credentials.class,
                run,
                (DomainRequirement) null);
        if (credential == null) {
            throw new AbortException(Messages.Rancher2RedeployBuilder_missCredential(credentialId));
        }

        String endpoint = credential.getEndpoint();
        if (endpoint.endsWith("/")) {
            endpoint = endpoint.substring(0, endpoint.length() - 1);
        }
        String url = endpoint + envVars.expand(workload);
        if (url.startsWith("/p/")) {
            url = url.replaceFirst("/p/", "/project/").replaceFirst("/workload/", "/workloads/");
        }

        Set<String> currentDeployPods = null;
        try (CloseableHttpClient client = ClientBuilder.create(endpoint, credential.isTrustCert())) {
            if(pollingDeployFinish) {
                currentDeployPods = getWorkloadPods(logger, client, credential, url);
            }

            if (StringUtils.isBlank(images)) {
                HttpUriRequest request = RequestBuilder.post(url + "?action=redeploy")
                        .addHeader("Authorization", "Bearer " + credential.getBearerToken())
                        .addHeader("Accept", "application/json")
                        .build();

                CloseableHttpResponse response = client.execute(request);
                if (response.getStatusLine().getStatusCode() != 200) {
                    throw new AbortException(
                            Messages.Rancher2RedeployBuilder_badResponse(
                                    response.getStatusLine().getStatusCode(),
                                    EntityUtils.toString(response.getEntity())
                            )
                    );
                }
            } else {
                putConfigRedeploy(logger, client, envVars, credential, url);
            }

            if(currentDeployPods != null && currentDeployPods.size() > 0) {
                pollingCheckPodsDeployFinish(logger, client, credential, url, currentDeployPods);
            }
            logger.println(Messages._Rancher2RedeployBuilder_success());
        }
    }

    private void putConfigRedeploy(PrintStream logger, CloseableHttpClient client, EnvVars envVars, Rancher2Credentials credential, String url) throws InterruptedException, IOException {
        Map<String, String> imageTags = new HashMap<>();
        if (StringUtils.isNotBlank(images)) {
            String expandImages = envVars.expand(images);
            String[] imageArray = expandImages.split(";");
            for (String imageTag : imageArray) {
                String name = parseImageName(imageTag);
                imageTags.put(name, imageTag);
            }
        }
        Set<String> workloadImages = new HashSet<>();
        Set<String> updatedImages = new HashSet<>();

        HttpUriRequest request = RequestBuilder.get(url)
                .addHeader("Authorization", "Bearer " + credential.getBearerToken())
                .addHeader("Accept", "application/json")
                .build();

        CloseableHttpResponse response = client.execute(request);
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new AbortException(
                    Messages.Rancher2RedeployBuilder_badResponse(
                            response.getStatusLine().getStatusCode(),
                            EntityUtils.toString(response.getEntity())
                    )
            );
        }

        // modify json body for PUT request
        JsonNode root = MAPPER.readTree(response.getEntity().getContent());
        ObjectNode objectNode = (ObjectNode) root;
        objectNode.remove("actions");
        objectNode.remove("links");
        //annotations
        ObjectNode annotations = (ObjectNode) root.get("annotations");
        String timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date());
        if (annotations != null) {
            annotations.put("cattle.io/timestamp", timestamp);
        }
        JsonNode containers = root.get("containers");
        for (int i = 0; containers != null && i < containers.size(); i++) {
            ObjectNode container = (ObjectNode) containers.get(i);
            String oldTag = container.get("image").asText();
            if (oldTag != null) {
                String name = parseImageName(oldTag);
                workloadImages.add(name);
                if (imageTags.containsKey(name)) {
                    String newTag = imageTags.get(name);
                    container.put("image", newTag);
                    if (alwaysPull) {
                        container.put("imagePullPolicy", "Always");
                    }
                    logger.println(Messages.Rancher2RedeployBuilder_setImageTag(oldTag, newTag));
                    updatedImages.add(name);
                }
            }
        }

        if (updatedImages.size() != imageTags.size()) {
            throw new AbortException(Messages.Rancher2RedeployBuilder_notMatch(workloadImages, imageTags.keySet()));
        }

        HttpUriRequest putRequest = RequestBuilder.put(url)
                .addHeader("Authorization", "Bearer " + credential.getBearerToken())
                .addHeader("Accept", "application/json")
                .addHeader("Content-Type", "application/json; charset=utf-8")
                .setEntity(new StringEntity(MAPPER.writeValueAsString(root), "utf-8"))
                .build();

        CloseableHttpResponse putResponse = client.execute(putRequest);
        if (putResponse.getStatusLine().getStatusCode() != 200) {
            throw new AbortException(Messages.Rancher2RedeployBuilder_badResponse(
                    response.getStatusLine().getStatusCode(),EntityUtils.toString(response.getEntity())
            ));
        }
    }

    /**
     * @param imageTag
     * @return image name without version
     */
    private static String parseImageName(String imageTag) {
        int index = imageTag.lastIndexOf(":");
        if (index < 0) {
            return imageTag;
        }
        return imageTag.substring(0, index);
    }

    @Symbol("rancherRedeploy")
    @Extension
    public static final class DescriptorImpl extends BuildStepDescriptor<Builder> {

        public FormValidation doCheckWorkload(
                @QueryParameter String value) {
            if (StringUtils.isBlank(value)) {
                return FormValidation.error(Messages.Rancher2RedeployBuilder_DescriptorImpl_requireWorkloadPath());
            }
            if (!value.startsWith("/project") && !value.startsWith("/p/")) {
                return FormValidation.error(Messages.Rancher2RedeployBuilder_DescriptorImpl_startWithProject());
            }
            return FormValidation.ok();
        }

        public FormValidation doCheckImages(
                @QueryParameter String value) {
            if (StringUtils.isBlank(value)) {
                return FormValidation.ok();
            }
            String[] images = value.split(";");
            for (String image : images) {
                if (image.isEmpty()) {
                    return FormValidation.error(Messages.Rancher2RedeployBuilder_DescriptorImpl_redundantSemicolon());
                }
            }
            return FormValidation.ok();
        }

        @Override
        public boolean isApplicable(Class<? extends AbstractProject> aClass) {
            return true;
        }

        @Override
        public String getDisplayName() {
            return Messages.Rancher2RedeployBuilder_DescriptorImpl_displayName();
        }

        public ListBoxModel doFillCredentialItems(
                @AncestorInPath Item item,
                @QueryParameter String credential
        ) {
            StandardListBoxModel result = new StandardListBoxModel();
            if (item == null) {
                if (!Jenkins.get().hasPermission(Jenkins.ADMINISTER)) {
                    return result.includeCurrentValue(credential);
                }
            } else {
                if (!item.hasPermission(Item.EXTENDED_READ)
                        && !item.hasPermission(CredentialsProvider.USE_ITEM)) {
                    return result.includeCurrentValue(credential);
                }
            }

            return CredentialsProvider.listCredentials(Rancher2Credentials.class,
                    Jenkins.get(), ACL.SYSTEM, Collections.<DomainRequirement>emptyList(), CredentialsMatchers.always());
        }

        public FormValidation doCheckCredential(
                @AncestorInPath Item item, // (2)
                @QueryParameter String value
        ) {
            if (item == null) {
                if (!Jenkins.getInstance().hasPermission(Jenkins.ADMINISTER)) {
                    return FormValidation.ok(); // (3)
                }
            } else {
                if (!item.hasPermission(Item.EXTENDED_READ)
                        && !item.hasPermission(CredentialsProvider.USE_ITEM)) {
                    return FormValidation.ok(); // (3)
                }
            }
            if (StringUtils.isBlank(value)) { // (4)
                return FormValidation.ok(); // (4)
            }
            if (value.startsWith("${") && value.endsWith("}")) { // (5)
                return FormValidation.warning(Messages.Rancher2RedeployBuilder_DescriptorImpl_credentialsCannotValidate());
            }
            if (CredentialsProvider.listCredentials(
                    Rancher2Credentials.class,
                    item, null, null,
                    CredentialsMatchers.withId(value)).isEmpty()) {
                return FormValidation.error(Messages.Rancher2RedeployBuilder_DescriptorImpl_credentialsCannotFind());
            }
            return FormValidation.ok();
        }

        public FormValidation doCheckPollingDeployTimeout(
                @QueryParameter String value) {
            if (StringUtils.isBlank(value)) {
                return FormValidation.ok();
            }
            try {
                int timeout = Integer.parseInt(value);
                if (timeout <= 0 || timeout > 900) {
                    return FormValidation.error(Messages.Rancher2RedeployBuilder_DescriptorImpl_redundantSemicolon());
                }
            } catch (NumberFormatException e) {
                return FormValidation.error(Messages.Rancher2RedeployBuilder_DescriptorImpl_redundantSemicolon());
            }
            return FormValidation.ok();
        }
    }

}
