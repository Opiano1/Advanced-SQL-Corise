with  resource_ as (

     select *
     from resources.us_cities
    qualify (row_number() over (partition by city_name, state_abbr order by county_name ))=1
), 

customer_eligble as (

    select customers.customer_id
    from customers.customer_address customers
    join resource_
    on lower(trim(city_name)) = lower(trim(customer_city)) 
        and lower(trim(state_abbr)) = lower(trim(customer_state))
), 

customer_base as (
    select 
        customer_id , tag_property , row_number() over 
            (partition by survey.customer_id 
                order by tag_property asc) as food_pref
    from customers.customer_survey survey
        inner join resources.recipe_tags recipe 
        on survey.tag_id = recipe.tag_id
        inner join customer_eligble
        on customer_eligble.customer_id =  survey.customer_id
    where survey.is_active = true
), 

food_pref as (
    select *
    from customer_base
    pivot(max(tag_property) 
          for food_pref in (1, 2, 3))
          as pivot_values (customer_id, food_pref_1, food_pref_2, food_pref_3)
), 


tag_per_recipe as (
    select 
        recipe_tag
        , max(recipe_name) as suggested_recipe
    from (
          select 
             recipe_name
             , trim(replace(flat_tag.value, '"', '')) as recipe_tag
          from chefs.recipe, table(flatten(tag_list)) as flat_tag
         ) as recipe 

    group by 1
)

select 
    food_pref.customer_id ,  customers.email, customers.first_name,
    food_pref.food_pref_1, food_pref.food_pref_2, food_pref.food_pref_3,
    recipe.suggested_recipe
from food_pref 
left join tag_per_recipe recipe 
    on recipe.recipe_tag = food_pref.food_pref_1
left join customers.customer_data customers 
    on customers.customer_id = food_pref.customer_id
