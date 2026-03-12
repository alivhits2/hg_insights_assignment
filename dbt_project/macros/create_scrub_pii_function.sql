
{% macro create_scrub_pii_function() %}
{% set sql %}
CREATE OR REPLACE FUNCTION scrub_pii(input_text TEXT)
RETURNS TEXT AS $$
DECLARE
result TEXT;
BEGIN
  result := input_text;

  -- Email
  result := REGEXP_REPLACE(
    result,
    '[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}',
    '[EMAIL]',
    'g'
  );

  -- Phone
  result := REGEXP_REPLACE(
    result,
    '(\+1[\s\-]?)?\(?[0-9]{3}\)?[\s\.\-]?[0-9]{3}[\s\.\-]?[0-9]{4}',
    '[PHONE]',
    'g'
  );

  -- SSN
  result := REGEXP_REPLACE(
    result,
    '\m[0-9]{3}[\s\-]?[0-9]{2}[\s\-]?[0-9]{4}\M',
    '[SSN]',
    'g'
  );

  -- IP Address
  result := REGEXP_REPLACE(
    result,
    '\m([0-9]{1,3}\.){3}[0-9]{1,3}\M',
    '[IP_ADDRESS]',
    'g'
  );

  -- Credit Card
  result := REGEXP_REPLACE(
    result,
    '\m(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12})\M',
    '[CREDIT_CARD]',
    'g'
  );

RETURN result;
END;
$$ LANGUAGE plpgsql;
{% endset %}
{{ run_query(sql) }}
{% endmacro %}
