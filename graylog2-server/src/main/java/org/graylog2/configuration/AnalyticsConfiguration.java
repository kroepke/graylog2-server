package org.graylog2.configuration;

import com.github.joschi.jadconfig.Parameter;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

public class AnalyticsConfiguration {

    @Parameter(value = "analytics_enabled")
    private boolean analyticsEnabled = true;

    @Parameter(value = "analytics_api_key")
    private String analyticsApiKey = "phc_eiHQiGawTT5ARAzVJmUN0NPbAXhU1yxArKAietTrQWf";

    @Parameter(value = "analytics_api_host")
    private String analyticsApiHost = "https://app.posthog.com";

    public boolean isAnalyticsEnabled() {
        return analyticsEnabled;
    }

    @Nullable
    public String getAnalyticsApiKey() {
        return analyticsApiKey;
    }

    @Nullable
    public String getAnalyticsApiHost() {
        return analyticsApiHost;
    }

    public Map<String, ?> toMap() {
        return Map.of(
                "api_key", getAnalyticsApiKey() != null ? getAnalyticsApiKey() : "",
                "host", getAnalyticsApiHost() != null ? getAnalyticsApiHost() : "",
                "enabled", isAnalyticsEnabled()
        );
    }
}
