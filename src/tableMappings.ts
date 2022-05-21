import { RETAIN } from "./constants";
import {
  replaceEmailBasedOnColumn,
  replaceWithNull,
  replaceWithScrambledText,
} from "./filters";

export const tableMappings = {
  outschool_learner_pii: {
    uid: RETAIN,
    email_address: replaceEmailBasedOnColumn("uid"),
    personal_info: replaceWithScrambledText,
    secret_token: replaceWithNull,
  },
};
