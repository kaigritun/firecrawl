import { config } from "../../config";
import {
  SearchResult,
  SearchV2Response,
  SearchResultType,
} from "../../lib/entities";
import * as Sentry from "@sentry/node";
import { logger } from "../../lib/logger";
import { executeWithRetry, attemptRequest } from "../../lib/retry-utils";

const useFireEngine =
  config.FIRE_ENGINE_BETA_URL !== "" &&
  config.FIRE_ENGINE_BETA_URL !== undefined;

export async function fire_engine_search_v2(
  q: string,
  options: {
    tbs?: string;
    filter?: string;
    lang?: string;
    country?: string;
    location?: string;
    numResults: number;
    page?: number;
    type?: SearchResultType | SearchResultType[];
    enterprise?: ("default" | "anon" | "zdr")[];
  },
  abort?: AbortSignal,
): Promise<SearchV2Response> {
  if (!useFireEngine) {
    logger.warn(
      "FIRE_ENGINE_BETA_URL is not configured, returning empty search results",
    );
    return {};
  }

  const payload = {
    query: q,
    lang: options.lang,
    country: options.country,
    location: options.location,
    tbs: options.tbs,
    numResults: options.numResults,
    page: options.page ?? 1,
    type: options.type || "web",
    enterprise: options.enterprise,
  };

  const url = `${config.FIRE_ENGINE_BETA_URL}/v2/search`;
  const data = JSON.stringify(payload);

  const result = await executeWithRetry<SearchV2Response>(
    () => attemptRequest<SearchV2Response>(url, data, abort),
    (response): response is SearchV2Response => response !== null,
    abort,
  );

  return result ?? {};
}
