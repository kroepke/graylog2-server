/**
 * Copyright 2013 Lennart Koopmann <lennart@torch.sh>
 *
 * This file is part of Graylog2.
 *
 * Graylog2 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Graylog2 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Graylog2.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
package org.graylog2.radio;

import com.google.common.io.Resources;
import org.graylog2.plugin.Version;

import java.io.FileReader;
import java.net.URL;
import java.util.Properties;

/**
 * @author Lennart Koopmann <lennart@torch.sh>
 */
public class RadioVersion {
    public static final Version vDEV = new Version(0, 20, 0, "dev");
    public static final Version v0_20_0_PREVIEW_7 = new Version(0, 20, 0, "preview.7");
    public static final Version v0_20_0_RC_1 = new Version(0, 20, 0, "rc.1");
    public static final Version v0_20_0_RC_1_1 = new Version(0, 20, 0, "rc.1-1");

    public static final Version v0_20_0 = new Version(0, 20, 0);

    public static final Version v0_20_2_SNAPSHOT = new Version(0, 20, 2, "snapshot");
    public static final Version v0_21_0_SNAPSHOT = new Version(0, 21, 0, "snapshot");
    public static final Version v0_20_2_RC_1 = new Version(0, 20, 2, "rc.1");
    public static final Version v0_20_2 = new Version(0, 20, 2);

    public static final Version VERSION;
    public static final String CODENAME = "Moose";

    static {
        Version tmpVersion;
        try {
            final URL resource = Resources.getResource("version.properties");
            final FileReader versionProperties = new FileReader(resource.getFile());
            final Properties version = new Properties();
            version.load(versionProperties);
            final int major = Integer.parseInt(version.getProperty("version.major", "0"));
            final int minor = Integer.parseInt(version.getProperty("version.minor", "0"));
            final int incremental = Integer.parseInt(version.getProperty("version.incremental", "0"));
            final String qualifier = version.getProperty("version.qualifier", "unknown");
            tmpVersion = new Version(major, minor, incremental, qualifier);
        } catch (Exception e) {
            tmpVersion = new Version(0, 0, 0, "unknown");
            System.err.println("Unable to read version.properties file, this build has no version number.");
        }
        VERSION = tmpVersion;
    }
}
