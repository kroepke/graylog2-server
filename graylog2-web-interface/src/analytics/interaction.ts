import type { PostHog } from 'posthog-js';

function getCaptureGroups(elem: Element, prevCaptureGroups: string[] = []): string[] {
  const captureGroups = [...prevCaptureGroups];

  if (elem.hasAttribute('data-capture-group')) {
    captureGroups.push(elem.getAttribute('data-capture-group'));
  }

  if (elem.parentElement) {
    return getCaptureGroups(elem.parentElement, captureGroups);
  }

  return captureGroups;
}

function getElementProperties(elem: Element) {
  const tag_name = elem.tagName.toLowerCase();

  const props = {
    tag_name: tag_name,
    el_text: elem.textContent,
    classes: Array.from(elem.classList).filter((c) => c !== ''),
    capture_groups: getCaptureGroups(elem),
  };

  Array.from(elem.attributes).forEach((attr) => {
    props[`attr_${attr.name}`] = attr.value;
  });

  return props;
}

function findOrigin(target: EventTarget, tagsToCatch: string[]): Element | undefined {
  for (let i = 0; i < tagsToCatch.length; i += 1) {
    if (target instanceof Element) {
      const o = target.closest(tagsToCatch[i]);

      if (o) {
        return o;
      }
    }
  }

  return undefined;
}

function interactionBinding(posthog: PostHog) {
  const eventsToCatch = ['click', 'submit', 'change'];
  const tagsToCatch = ['a', 'button', 'input', 'select', 'textarea', 'label', 'form'];

  eventsToCatch.forEach((eventType) => {
    document.addEventListener(eventType, (e) => {
      const origin = findOrigin(e.target, tagsToCatch);

      if (origin) {
        // found an element of tag to capture
        posthog.capture(
          `${eventType}: ${origin.tagName.toLowerCase()}`,
          {
            ...getElementProperties(origin),
            event_type: e.type,
          },
        );
      }
    });
  });
}

export default interactionBinding;
