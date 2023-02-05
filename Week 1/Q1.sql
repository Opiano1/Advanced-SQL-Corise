with customers as (
    select 
        customer_data.customer_id,
        customer_data.first_name,
        customer_data.last_name, 
        customer_data.email, 
        address_id as customer_address_id,
        lower(customer_city) as customer_city,
        lower(customer_state) as customer_state
        
    from vk_data.customers.customer_data
    inner join vk_data.customers.customer_address 
        on customer_data.customer_id = customer_address.customer_id 
),

suppliers as (

    select 
        supplier_id,
        supplier_name,
        
        lower(supplier_city) as supplier_city,
        lower(supplier_state) as supplier_state_abbr
    from vk_data.suppliers.supplier_info
),

customer_geo as (
    select 
        lower(city_name) as city_name,
        lower(state_abbr) as state_abbr,
        geo_location
    from vk_data.resources.us_cities
),

geo_suppliers as (
    select 
        suppliers.*,
        customer_geo.geo_location as supplier_geo
    from suppliers 
    inner join customer_geo 
        on suppliers.supplier_city = customer_geo.city_name 
        and suppliers.supplier_state_abbr = customer_geo.state_abbr 
),

geo_customers as (
    select 
        customers.*,
        customer_geo.geo_location as customer_geo
    from customers 
    inner join customer_geo 
        on customers.customer_city = customer_geo.city_name
        and customers.customer_state = customer_geo.state_abbr
),

calc_distance as (
    select *, (st_distance(customer_geo, supplier_geo) / 1000) as distance_km
    from geo_customers
    cross join geo_suppliers
),

distance_rank as (
    select  *,
       
    from calc_distance 
    qualify ( rank() over (
            partition by customer_id 
            order by distance_km
            ) as shipping_dt_rnk) = 1
)

select customer_id, first_name,last_name,
        email, supplier_id, supplier_name, distance_km
from distance_rank 