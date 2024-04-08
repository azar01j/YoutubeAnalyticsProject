with coco_mel_stg as (
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
"timestamp"::datetime as timestamp,

from {{ ref('Cocomelon_Raw') }}
),
vj_sidhu_stg as (
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
"timestamp"::datetime as timestamp,

from {{ ref('VJ_Siddhu_Vlogs_') }}
),
tseries_ as (
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
"timestamp"::datetime as timestamp,

from {{ ref('TSeries_') }}
),
PewDiePie_ as (
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
from {{ ref('PewDiePie_') }}
),
Madan_gowri_stg as (
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
"timestamp"::datetime as timestamp,

from {{ ref('Madan_Gowri_') }}
),
mrbeast_ as (
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
from {{ ref('MrBeast_') }}
),Jungle_ as (
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
from {{ ref('Jungle_Toons_') }}
)

select * from coco_mel_stg
union all
select * from vj_sidhu_stg
union all 
select * from tseries_
union all 
select * from PewDiePie_
union all 
select * from Madan_gowri_stg
union all
select * from mrbeast_
union all
select * from Jungle_