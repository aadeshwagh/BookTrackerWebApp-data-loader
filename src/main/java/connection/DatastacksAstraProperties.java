package connection;

import java.io.File;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "datastax.astra")
public class DatastacksAstraProperties {
    private File secureconnectbundle;

    public File getSecureconnectbundle() {
        return secureconnectbundle;
    }

    public void setSecureconnectbundle(File secureconnectbundle) {
        this.secureconnectbundle = secureconnectbundle;
    }

}
