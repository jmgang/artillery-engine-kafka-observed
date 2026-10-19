import type { KafkaJS } from '@confluentinc/kafka-javascript';
import { generateAuthToken } from 'aws-msk-iam-sasl-signer-js';

/** none: local PLAINTEXT broker; tls: TLS only; iam: Amazon MSK IAM over SASL/OAUTHBEARER + TLS. */
export type KafkaAuthMode = 'none'|'tls'|'iam';
export interface ConnectionOptions { brokers:string[];clientId:string;auth?:KafkaAuthMode;region?:string; }
export type TokenProvider = (region:string)=>Promise<{token:string;expiryTime:number}>;

/** Empty or missing (e.g. an unset `{{ $env.KAFKA_AUTH }}` in an Artillery script) means none. */
export function parseAuthMode(value:unknown):KafkaAuthMode{
  const mode=value===undefined||value===null||String(value).trim()===''?'none':String(value).trim().toLowerCase();
  if(mode==='none'||mode==='tls'||mode==='iam')return mode;
  throw new Error('KAFKA_AUTH must be none, tls or iam');
}

/** KafkaJS-compatible logLevel.ERROR. */
const LOG_ERROR=1 as KafkaJS.logLevel;

/** @confluentinc/kafka-javascript configuration (KafkaJS-compatible API) for the producer and Admin client. */
export function kafkaConfig(o:ConnectionOptions,tokens:TokenProvider=region=>generateAuthToken({region})):KafkaJS.CommonConstructorConfig{
  if(!o.brokers.length)throw new Error('At least one Kafka broker is required');
  const auth=o.auth??'none';
  const base:KafkaJS.KafkaConfig={brokers:o.brokers,clientId:o.clientId,logLevel:LOG_ERROR};
  if(auth==='none')return {kafkaJS:base};
  if(auth==='tls')return {kafkaJS:{...base,ssl:true}};
  const region=o.region||process.env.AWS_REGION||process.env.AWS_DEFAULT_REGION;
  if(!region)throw new Error('MSK IAM authentication requires a region (option or AWS_REGION)');
  // Credentials come from the default AWS chain: ECS task role, instance profile, environment or profile.
  const sasl:KafkaJS.SASLOptions={mechanism:'oauthbearer',oauthBearerProvider:async()=>{
    const signed=await tokens(region);return {value:signed.token,principal:'msk-iam',lifetime:signed.expiryTime};
  }};
  return {kafkaJS:{...base,ssl:true,sasl}};
}
