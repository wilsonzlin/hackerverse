import { VDate, VOptional, VString, VStruct, Valid } from "@wzlin/valid";

export const vUrlMeta = new VStruct({
  description: new VOptional(new VString()),
  imageUrl: new VOptional(new VString()),
  lang: new VOptional(new VString()),
  snippet: new VOptional(new VString()),
  timestamp: new VOptional(new VDate()),
  timestampModified: new VOptional(new VDate()),
  title: new VOptional(new VString()),
});
export type UrlMeta = Valid<typeof vUrlMeta>;
