import { RETAIN } from "./constants";
import {
  replaceEmailWithColumn,
  replaceWithNull,
  replaceWithScrambledText,
} from "./filters";

export const tableMappings = {
  outschool_learner_pii: {
    uid: RETAIN,
    email_address: replaceEmailWithColumn("uid"),
    personal_info: replaceWithScrambledText,
    secret_token: replaceWithNull,
  },
};
