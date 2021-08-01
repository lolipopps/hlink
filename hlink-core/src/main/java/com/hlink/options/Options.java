package com.hlink.options;

import com.hlink.constants.ConfigConstant;
import com.hlink.constants.ConstantValue;
import com.hlink.enums.ClusterMode;
import com.hlink.enums.ConnectorLoadMode;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

@Data
public class Options {
    @OptionRequired(description = "job type:sql or sync")
    private String jobType;

    @OptionRequired(description = "Running mode")
    private String mode = ClusterMode.local.name();

    @OptionRequired(description = "Job config")
    private String job;

    @OptionRequired(description = "Flink Job Name")
    private String jobName = "Flink Job";

    @OptionRequired(description = "Flink configuration directory")
    private String flinkconf;

    @OptionRequired(description = "env properties")
    private String pluginRoot;

    @OptionRequired(description = "Yarn and Hadoop configuration directory")
    private String yarnconf;

    @OptionRequired(description = "Task parallelism")
    private String parallelism = "1";

    @OptionRequired(description = "Task priority")
    private String priority = "1";

    @OptionRequired(description = "Yarn queue")
    private String queue = "default";

    @OptionRequired(description = "ext flinkLibJar")
    private String flinkLibJar;

    @OptionRequired(description = "env properties")
    private String confProp = "{}";

    @OptionRequired(description = "savepoint path")
    private String s;

    @OptionRequired(description = "plugin load mode, by classpath or shipfile")
    private String pluginLoadMode = "shipfile";

    @OptionRequired(description = "kerberos krb5conf")
    private String krb5conf;

    @OptionRequired(description = "kerberos keytabPath")
    private String keytab;

    @OptionRequired(description = "kerberos principal")
    private String principal;

    @OptionRequired(description = "applicationId on yarn cluster")
    private String appId;

    @OptionRequired(description = "Sync remote plugin root path")
    private String remotePluginPath;

    @OptionRequired(description = "sql ext jar,eg udf jar")
    private String addjar;

    @OptionRequired(description = "file add to ship file")
    private String addShipfile;

    @OptionRequired(description = "connectorLoadMode spi or reflect")
    private String connectorLoadMode = ConnectorLoadMode.CLASSLOADER.name();

    private Configuration flinkConfiguration = null;

    public Configuration loadFlinkConfiguration() {
        if (flinkConfiguration == null) {
            flinkConfiguration = StringUtils.isEmpty(flinkconf) ? new Configuration() : GlobalConfiguration.loadConfiguration(flinkconf);
            if (StringUtils.isNotBlank(queue)) {
                flinkConfiguration.setString(YarnConfigOptions.APPLICATION_QUEUE, queue);
            }
            if (StringUtils.isNotBlank(jobName)) {
                flinkConfiguration.setString(YarnConfigOptions.APPLICATION_NAME, jobName);
            }
            if (StringUtils.isNotBlank(yarnconf)) {
                flinkConfiguration.setString(ConfigConstants.PATH_HADOOP_CONFIG, yarnconf);
            }
            if (ConstantValue.CLASS_PATH_PLUGIN_LOAD_MODE.equalsIgnoreCase(pluginLoadMode)) {
                flinkConfiguration.setString(CoreOptions.CLASSLOADER_RESOLVE_ORDER, "child-first");
            } else {
                flinkConfiguration.setString(CoreOptions.CLASSLOADER_RESOLVE_ORDER, "parent-first");
            }
            flinkConfiguration.setString(ConfigConstant.FLINK_PLUGIN_LOAD_MODE_KEY, pluginLoadMode);
        }
        return flinkConfiguration;
    }

}
