import posthog from 'posthog-js';

import AppConfig from 'util/AppConfig';

import userBindings from './user';
import navigationBindings from './navigation';
import interactionBinding from './interaction';

const POSTHOG_DEBUG = false;

function getPostHogSettings(): { host: string; key: string; debug: boolean; isDisabled: boolean } {
  const { api_key: key, host: host } = AppConfig.analytics();

  const isDisabled = !AppConfig.analytics().enabled || !key || !host;

  return {
    host,
    key,
    debug: POSTHOG_DEBUG,
    isDisabled,
  };
}

function onPostHogLoaded() {
  userBindings(posthog);
  navigationBindings(posthog);
  interactionBinding(posthog);
}

function init() {
  const { host, key, debug, isDisabled } = getPostHogSettings();

  if (isDisabled) {
    return;
  }

  posthog.init(
    key,
    {
      autocapture: false,
      api_host: host,
      loaded: onPostHogLoaded,
    },
  );

  if (debug) {
    posthog.debug();
  } else {
    // There is no way to disable debug mode in posthog.js once it has been enabled,
    // so we need to do it manually by removing the localStorage key.
    try {
      window.localStorage.removeItem('ph_debug');
    } catch (e) {
      // ignore
    }
  }
}

export default init;
