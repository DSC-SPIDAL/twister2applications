package edu.iu.dsc.tws.mpiapps.configuration;


import edu.iu.dsc.tws.mpiapps.configuration.section.SmartHomeSection;

public class ConfigurationMgr {
    private String configurationFilePath;
    public SmartHomeSection smartHomesSection;

    public ConfigurationMgr(String configurationFilePath) {
        this.configurationFilePath = configurationFilePath;
        smartHomesSection = new SmartHomeSection(configurationFilePath);


    }

    public static ConfigurationMgr LoadConfiguration(String configurationFilePath){
        // TODO - Fix configuration management
        return new ConfigurationMgr(configurationFilePath);
    }
}
