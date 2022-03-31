import { RETAIN } from "./constantsAndTypes";
import {
  replaceEmailBasedOnColumn,
  replaceWithNull,
  replaceWithScrambledText,
} from "./helpers";

export const tableMappings = {
  learner_pii: {
    uid: RETAIN,
    email_address: replaceEmailBasedOnColumn("uid"),
    personal_info: replaceWithScrambledText,
    secret_token: replaceWithNull,
  },
};
