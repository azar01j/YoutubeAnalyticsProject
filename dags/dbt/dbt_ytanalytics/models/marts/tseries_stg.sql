with tseries_ as (
select 
"title"::varchar(50) as title,
"customUrl"::varchar(50) as customUrl,
"publishedAt"::datetime as PublishedAt,
"url"::varchar(300) as url_,
"country"::varchar(10) as Country,
"viewCount"::int as view_count,
"subscriberCount"::int as subscriberCount,
"videoCount"::int as videoCount,
"madeForKids"::Boolean as madeForKids,
"timestamp"::datetime as timestamp
from {{ ref('TSeries_') }}
)

select * from tseries_
