drop table if exists ${catalog}.customer.profile;
create table ${catalog}.customer.profile (
    id string comment 'id',
    name string comment 'name',
    gender string comment 'gender',
    email string comment 'email',
    customer_type string comment 'customer type',
    customer_status boolean comment 'is active',
    constraint primary key (id)
)
using delta
partitioned by (customer_type);
location 's3://${bucket}/customer/profile';
comment 'customer profile
# Description
This is a table that stores customer profile information'
;

alter table ${catalog}.customer.profile add column tel string comment 'telephone number';

alter table ${catalog}.customer.profile drop column gender;

alter table ${catalog}.customer.profile drop column customer_status;
