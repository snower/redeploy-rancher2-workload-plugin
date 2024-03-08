package io.jenkins.plugins.rancher2;

import com.cloudbees.plugins.credentials.CredentialsMatchers;
import com.cloudbees.plugins.credentials.CredentialsProvider;
import com.cloudbees.plugins.credentials.common.StandardListBoxModel;
import com.cloudbees.plugins.credentials.domains.DomainRequirement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.umd.cs.findbugs.annotations.NonNull;
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
import io.jenkins.cli.shaded.org.apache.commons.io.IOUtils;
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
import org.kohsuke.stapler.DataBoundSetter;
import org.kohsuke.stapler.QueryParameter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import jenkins.tasks.SimpleBuildStep;
import org.jenkinsci.Symbol;

public class Rancher2RedeployBuilder extends Builder implements SimpleBuildStep {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Nonnull
    private final String credential;
    @Nonnull
    private final String workload;
    private final String images;
    private final boolean alwaysPull;
    private boolean pollingDeployFinish = false;
    private int pollingDeployTimeout = 300;
    private String templateUrl = null;
    private String templateVars = null;

    @DataBoundConstructor
    public Rancher2RedeployBuilder(
            @Nonnull String credential,
            @Nonnull String workload,
            @Nullable String images,
            boolean alwaysPull
    ) {
        this.credential = credential;
        this.workload = workload;
        this.images = images;
        this.alwaysPull = alwaysPull;
    }

    @DataBoundSetter
    public void setPollingDeployFinish(@Nullable Boolean pollingDeployFinish) {
        this.pollingDeployFinish = pollingDeployFinish != null && pollingDeployFinish;
    }

    @DataBoundSetter
    public void setPollingDeployTimeout(@Nullable Integer pollingDeployTimeout) {
        this.pollingDeployTimeout = pollingDeployTimeout == null ? 300 : pollingDeployTimeout;
    }

    @DataBoundSetter
    public void setTemplateUrl(@Nullable String templateUrl) {
        this.templateUrl = templateUrl;
    }

    @DataBoundSetter
    public void setTemplateVars(@Nullable String templateVars) {
        this.templateVars = templateVars;
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

    public String getTemplateUrl() {
        return templateUrl;
    }

    public String getTemplateVars() {
        return templateVars;
    }

    private Set<String> getWorkloadPods(PrintStream logger, CloseableHttpClient client, Rancher2Credentials credential, String url, String selectedState) throws InterruptedException, IOException {
        String[] urlInfo = url.split("\\/workloads\\/");
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
            JsonNode pod = pods.get(i);
            String podState = pod.get("state").asText();
            if (StringUtils.isNotBlank(selectedState) && StringUtils.isNotBlank(podState) &&
                    !Objects.equals(podState.toLowerCase(), selectedState)) continue;
            String podID = pod.get("id").asText();
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
            Set<String> deployPods = getWorkloadPods(logger, client, credential, url, null);
            deployPods.retainAll(lastDeployPods);
            if(deployPods.size() <= 0) {
                return;
            }

            Thread.sleep(3000);
        }
        throw new AbortException(Messages.Rancher2RedeployBuilder_pollingDeployTimeout((int)(((new Date()).getTime() - startTime)/1000)));
    }

    private void pollingWaitPodsDeployFinish(PrintStream logger, CloseableHttpClient client, Rancher2Credentials credential, String url) throws InterruptedException, IOException {
        long startTime = (new Date()).getTime();
        Thread.sleep(5000);
        while (((new Date()).getTime() - startTime) < ((long) pollingDeployTimeout) * 1000) {
            Set<String> deployPods = getWorkloadPods(logger, client, credential, url, "running");
            if(deployPods.size() > 0) {
                return;
            }

            Thread.sleep(3000);
        }
        throw new AbortException(Messages.Rancher2RedeployBuilder_pollingDeployTimeout((int)(((new Date()).getTime() - startTime)/1000)));
    }

    @Override
    public void perform(@NonNull Run<?, ?> run, @NonNull FilePath workspace, @NonNull EnvVars envVars, @NonNull Launcher launcher, @NonNull TaskListener listener) throws InterruptedException, IOException {
        PrintStream logger = listener.getLogger();
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
                currentDeployPods = getWorkloadPods(logger, client, credential, url, null);
            }

            if (StringUtils.isBlank(images)) {
                putActionRedeploy(logger, client, envVars, credential, url);
            } else {
                putConfigRedeploy(logger, client, envVars, credential, url);
            }

            if(currentDeployPods != null && currentDeployPods.size() > 0) {
                pollingCheckPodsDeployFinish(logger, client, credential, url, currentDeployPods);
            }
            logger.println(Messages._Rancher2RedeployBuilder_success());
        }
    }

    private void putActionRedeploy(PrintStream logger, CloseableHttpClient client, EnvVars envVars, Rancher2Credentials credential, String url)  throws InterruptedException, IOException {
        HttpUriRequest request = RequestBuilder.post(url + "?action=redeploy")
                .addHeader("Authorization", "Bearer " + credential.getBearerToken())
                .addHeader("Accept", "application/json")
                .build();

        CloseableHttpResponse response = client.execute(request);
        if (response.getStatusLine().getStatusCode() != 200) {
            String message = Messages.Rancher2RedeployBuilder_badResponse(
                    response.getStatusLine().getStatusCode(),
                    EntityUtils.toString(response.getEntity())
            );
            if (response.getStatusLine().getStatusCode() == 404) {
                logger.println(message);
                if(postConfigDeploy(logger, client, envVars, credential, url)) return;
            }
            throw new AbortException(message);
        }
    }

    private void putConfigRedeploy(PrintStream logger, CloseableHttpClient client, EnvVars envVars, Rancher2Credentials credential, String url) throws InterruptedException, IOException {
        Map<String, String> imageTags = new HashMap<>();
        String expandImages = null;
        if (StringUtils.isNotBlank(images)) {
            expandImages = envVars.expand(images);
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
            String message = Messages.Rancher2RedeployBuilder_badResponse(
                    response.getStatusLine().getStatusCode(),
                    EntityUtils.toString(response.getEntity())
            );
            if (response.getStatusLine().getStatusCode() == 404) {
                logger.println(message);
                if(postConfigDeploy(logger, client, envVars, credential, url)) return;
            }
            throw new AbortException(message);
        }

        // modify json body for PUT request
        JsonNode root = MAPPER.readTree(response.getEntity().getContent());
        JsonNode containers = root.get("containers");
        if (containers != null && containers.size() > 0) {
            String oldImage = containers.get(0).get("image").asText();
            if (Objects.equals(expandImages, oldImage)) {
                putActionRedeploy(logger, client, envVars, credential, url);
                return;
            }
        }

        ObjectNode objectNode = (ObjectNode) root;
        objectNode.remove("actions");
        objectNode.remove("links");
        //annotations
        ObjectNode annotations = (ObjectNode) root.get("annotations");
        String timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date());
        if (annotations != null) {
            annotations.put("cattle.io/timestamp", timestamp);
        }
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
                    putResponse.getStatusLine().getStatusCode(),EntityUtils.toString(putResponse.getEntity())
            ));
        }
    }

    private boolean postConfigDeploy(PrintStream logger, CloseableHttpClient client, EnvVars envVars, Rancher2Credentials credential, String url) throws InterruptedException, IOException {
        if (StringUtils.isBlank(templateUrl)) return false;
        String projectId, namespaceId, nameId;
        String[] urlInfo = url.split("\\/project\\/");
        urlInfo = urlInfo[1].split("\\/workloads\\/");
        projectId = urlInfo[0];
        urlInfo = urlInfo[1].split(":");
        namespaceId = urlInfo[1];
        nameId = urlInfo[2];

        String expandTemplateUrl = envVars.expand(templateUrl);
        logger.println(Messages._Rancher2CredentialsImpl_DescriptorImpl_createPodStartMessage(expandTemplateUrl));
        JsonNode template = expandTemplateUrl.toLowerCase().startsWith("http") ?
                loadHttpTemplate(client, envVars, expandTemplateUrl, projectId, namespaceId, nameId) :
                loadFileTemplate(envVars, expandTemplateUrl, projectId, namespaceId, nameId);
        if (template == null) {
            throw new IOException("template error");
        }

        HttpUriRequest postRequest = RequestBuilder.post(url.split("\\/workloads")[0] + "/workloads")
                .addHeader("Authorization", "Bearer " + credential.getBearerToken())
                .addHeader("Accept", "application/json")
                .addHeader("Content-Type", "application/json; charset=utf-8")
                .setEntity(new StringEntity(MAPPER.writeValueAsString(template), "utf-8"))
                .build();
        CloseableHttpResponse postResponse = client.execute(postRequest);
        String postResponseContent = EntityUtils.toString(postResponse.getEntity());
        logger.println(Messages._Rancher2CredentialsImpl_DescriptorImpl_createPodSuccedMessage(postResponse.getStatusLine(), postResponseContent));
        if (postResponse.getStatusLine().getStatusCode() == 201) {
            if(pollingDeployFinish) {
                pollingWaitPodsDeployFinish(logger, client, credential, url);
            }
            return true;
        }
        if (postResponse.getStatusLine().getStatusCode() != 200) {
            throw new AbortException(Messages.Rancher2RedeployBuilder_badResponse(
                    postResponse.getStatusLine().getStatusCode(), postResponseContent));
        }
        return true;
    }

    private JsonNode loadHttpTemplate(CloseableHttpClient client, EnvVars envVars, String url, String projectId, String namespaceId, String nameId) throws InterruptedException, IOException {
        HttpUriRequest request = RequestBuilder.get(url)
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
        String content = EntityUtils.toString(response.getEntity());
        return MAPPER.readTree(compileTemplate(envVars, projectId, namespaceId, nameId, content));
    }

    private JsonNode loadFileTemplate(EnvVars envVars, String filename, String projectId, String namespaceId, String nameId) throws InterruptedException, IOException {
        File file = new File(filename);
        FileInputStream fileInputStream = new FileInputStream(file);
        String content = IOUtils.toString(fileInputStream, StandardCharsets.UTF_8);
        fileInputStream.close();
        return MAPPER.readTree(compileTemplate(envVars, projectId, namespaceId, nameId, content));
    }

    private String compileTemplate(EnvVars envVars, String projectId, String namespaceId, String nameId, String templateContent) {
        Map<String, String> vars = new HashMap<>();
        vars.put("PROJECTID", projectId);
        vars.put("NAMESPACEID", namespaceId);
        vars.put("NAMEID", nameId);
        if (StringUtils.isNotBlank(images)) {
            String expandImages = envVars.expand(images);
            if (StringUtils.isNotBlank(expandImages)) {
                vars.put("IMAGE", expandImages.split(";")[0]);
            }
        }
        vars.put("IMAGEPULLPOLICY", alwaysPull ? "Always" : "IfNotPresent");

        if (StringUtils.isNotBlank(templateVars)) {
            String expandTemplateVars = envVars.expand(templateVars);
            for (String varValue : expandTemplateVars.split(",")) {
                String[] varValues = varValue.split("=");
                if (varValues.length >= 2) {
                    vars.put(varValues[0], varValues[1]);
                }
            }
        }
        return Util.replaceMacro(templateContent, vars);
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
