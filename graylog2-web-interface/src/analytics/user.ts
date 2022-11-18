import type { PostHog } from 'posthog-js';

import { CurrentUserStore } from 'stores/users/CurrentUserStore';
import { ClusterOverviewStore } from 'stores/cluster/ClusterOverviewStore';

function userBindings(posthog: PostHog) {
  ClusterOverviewStore.listen((clusterState) => {
    if (clusterState) {
      clusterState.
    }
  });
  // listen to user state changes
  CurrentUserStore.listen((state) => {
    if (state.currentUser) {
      const isAnonymous = state.currentUser.preferences.diagnosticsIsAnonymized;

      if (!isAnonymous) {
        // identify the current user if it updates and allows it
        posthog.identify(
          state.currentUser.id,
          {
            email: state.currentUser.email,
            username: state.currentUser.username,
            name: state.currentUser.full_name,
          },
        );

        return;
      }
    }

    // reset posthog if user is not logged in anymore or anonymized
    posthog.reset();
  });
}

export default userBindings;
