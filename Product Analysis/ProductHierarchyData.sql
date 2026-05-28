WITH cat_name_attr AS (
  SELECT attribute_id
  FROM bme.eav_attribute
  WHERE attribute_code = 'name'
  AND entity_type_id = 3
),

prod_name_attr AS (
  SELECT attribute_id
  FROM bme.eav_attribute
  WHERE attribute_code = 'name'
  AND entity_type_id = 4
),

prod_status_attr AS (
  SELECT attribute_id
  FROM bme.eav_attribute
  WHERE attribute_code = 'status'
  AND entity_type_id = 4
),

category_base AS (
  SELECT
    cce.entity_id,
    cce.row_id,
    cce.path,
    cce.level,
    name_attr.value                       AS name,
    COALESCE(active_attr.value, 1)        AS is_active,
    COALESCE(menu_attr.value, 1)          AS include_in_menu
  FROM bme.catalog_category_entity cce
  LEFT JOIN bme.catalog_category_entity_varchar name_attr
    ON cce.row_id = name_attr.row_id
    AND name_attr.attribute_id = (SELECT attribute_id FROM cat_name_attr)
    AND name_attr.store_id = 0
  LEFT JOIN bme.catalog_category_entity_int active_attr
    ON cce.row_id = active_attr.row_id
    AND active_attr.attribute_id = (SELECT attribute_id FROM bme.eav_attribute WHERE attribute_code = 'is_active' AND entity_type_id = 3)
    AND active_attr.store_id = 0
  LEFT JOIN bme.catalog_category_entity_int menu_attr
    ON cce.row_id = menu_attr.row_id
    AND menu_attr.attribute_id = (SELECT attribute_id FROM bme.eav_attribute WHERE attribute_code = 'include_in_menu' AND entity_type_id = 3)
    AND menu_attr.store_id = 0
),

product_categories AS (
  SELECT
    cpe.entity_id                         AS product_id,
    cpe.sku,
    pname.value                           AS product_name,
    cpe.type_id                           AS product_type,
    cpe.created_at                        AS product_created_at,
    cpe.updated_at                        AS product_updated_at,
    CASE WHEN status_attr.value = 2
         THEN 'disabled'
         ELSE 'enabled'
    END                                   AS product_status,
    CASE WHEN status_attr.value = 2
         THEN cpe.updated_at
         ELSE NULL
    END                                   AS disabled_at_approx,
    l2.name                               AS category_l2,
    l3.name                               AS category_l3,
    l4.name                               AS category_l4,
    l5.name                               AS category_l5,
    l6.name                               AS category_l6,
    CONCAT_WS(' > ',
      l2.name,
      l3.name,
      l4.name,
      l5.name,
      l6.name
    )                                     AS category_hierarchy

  FROM bme.catalog_product_entity cpe

  LEFT JOIN bme.catalog_product_entity_varchar pname
    ON pname.row_id = cpe.row_id
    AND pname.attribute_id = (SELECT attribute_id FROM prod_name_attr)
    AND pname.store_id = 0

  LEFT JOIN bme.catalog_product_entity_int status_attr
    ON status_attr.row_id = cpe.row_id
    AND status_attr.attribute_id = (SELECT attribute_id FROM prod_status_attr)
    AND status_attr.store_id = 0

  LEFT JOIN bme.catalog_category_product ccp
    ON ccp.product_id = cpe.entity_id

  LEFT JOIN bme.catalog_category_entity cce
    ON cce.entity_id = ccp.category_id

  LEFT JOIN category_base l2
    ON l2.entity_id = SUBSTRING_INDEX(SUBSTRING_INDEX(cce.path, '/', 2), '/', -1)
    AND cce.level >= 2
  LEFT JOIN category_base l3
    ON l3.entity_id = SUBSTRING_INDEX(SUBSTRING_INDEX(cce.path, '/', 3), '/', -1)
    AND cce.level >= 3
  LEFT JOIN category_base l4
    ON l4.entity_id = SUBSTRING_INDEX(SUBSTRING_INDEX(cce.path, '/', 4), '/', -1)
    AND cce.level >= 4
  LEFT JOIN category_base l5
    ON l5.entity_id = SUBSTRING_INDEX(SUBSTRING_INDEX(cce.path, '/', 5), '/', -1)
    AND cce.level >= 5
  LEFT JOIN category_base l6
    ON l6.entity_id = SUBSTRING_INDEX(SUBSTRING_INDEX(cce.path, '/', 6), '/', -1)
    AND cce.level >= 6
)

SELECT
  sku,
  MAX(product_name)                                                                      AS product_name,
  MAX(product_type)                                                                      AS product_type,
  MAX(product_status)                                                                    AS product_status,
  MAX(product_created_at)                                                                AS product_created_at,
  MAX(product_updated_at)                                                                AS product_updated_at,
  MAX(disabled_at_approx)                                                                AS disabled_at_approx,
  MAX(category_l2)                                                                       AS category_l2,
  MAX(category_l3)                                                                       AS category_l3,
  MAX(category_l4)                                                                       AS category_l4,
  MAX(category_l5)                                                                       AS category_l5,
  MAX(category_l6)                                                                       AS category_l6,
  GROUP_CONCAT(DISTINCT category_hierarchy ORDER BY category_hierarchy SEPARATOR ' | ')  AS category_hierarchies

FROM product_categories

GROUP BY sku

ORDER BY sku;
