with base_data as(

SELECT
    requests.*,    

    -- Top-level section: 'shop', 'account', 'pay'
    SUBSTRING_INDEX(TRIM(LEADING '/' FROM path), '/', 1)
        AS section,

    -- Sub-type: 'category', 'product', 'method', 'documents', etc.
    CASE
        WHEN path REGEXP '^/[^/]+/[^/]+(/|$)'
        THEN SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(LEADING '/' FROM path), '/', 2), '/', -1)
        ELSE NULL
    END
        AS sub_type,

    -- Category/product ID (numeric segment in position 3)
    CASE
        WHEN SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(LEADING '/' FROM path), '/', 3), '/', -1) REGEXP '^[0-9]+$'
        THEN CAST(
            SUBSTRING_INDEX(SUBSTRING_INDEX(TRIM(LEADING '/' FROM path), '/', 3), '/', -1)
            AS UNSIGNED
        )
        ELSE NULL
    END
        AS item_id,

    -- Slug: last segment
    CASE
        WHEN path REGEXP '^(/[^/]+){3,}'
        THEN SUBSTRING_INDEX(path, '/', -1)
        ELSE NULL
    END
        AS slug

FROM account.requests
where created_at >= date(sysdate())

  )
select section, sub_type, item_id, slug, count(1)
from base_data
group by section, sub_type, item_id, slug
