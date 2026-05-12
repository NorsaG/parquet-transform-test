USE transform_demo;

SELECT
  case_id,
  src_ts15,
  TO_TIMESTAMP(src_ts15, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX'['VV']'") AS parsed_ts15,
  DATE_FORMAT(TO_TIMESTAMP(src_ts15, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX'['VV']'"), 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS formatted_ts15,
  TO_TIMESTAMP(src_ts16, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX'['VV']'") AS parsed_ts16,
  DATE_FORMAT(TO_UTC_TIMESTAMP(TO_TIMESTAMP(src_ts16, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX'['VV']'"), 'UTC'), 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS formatted_ts16_utc
FROM source_input
WHERE case_id IN ('baseline_valid', 'ts15_offset_zone_mismatch', 'ts16_india');

