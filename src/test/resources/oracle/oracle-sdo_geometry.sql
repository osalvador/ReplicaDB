create table t_source_geo (
    mkt_id number primary key,
    name   varchar2(32),
    shape  sdo_geometry
);

create table t_sink_geo (
    mkt_id number primary key,
    name   varchar2(32),
    shape  sdo_geometry
);

insert into t_source_geo
values (1,
        'cola_a',
        SDO_GEOMETRY(2003, -- two-dimensional polygon
                     null, null, SDO_ELEM_INFO_ARRAY(1, 1003, 3), -- one rectangle (1003 = exterior)
                     SDO_ORDINATE_ARRAY(1, 1, 5, 7) -- only 2 points needed to
        -- define rectangle (lower left and upper right) with
        -- Cartesian-coordinate data
            ));

-- The next two INSERT statements create areas of interest for
-- Cola B and Cola C. These areas are simple polygons (but not
-- rectangles).

insert into t_source_geo
values (2,
        'cola_b',
        SDO_GEOMETRY(2003, -- two-dimensional polygon
                     null, null, SDO_ELEM_INFO_ARRAY(1, 1003, 1), -- one polygon (exterior polygon ring)
                     SDO_ORDINATE_ARRAY(5, 1, 8, 1, 8, 6, 5, 7, 5, 1)));

insert into t_source_geo
values (3,
        'cola_c',
        SDO_GEOMETRY(2003, -- two-dimensional polygon
                     null, null, SDO_ELEM_INFO_ARRAY(1, 1003, 1), -- one polygon (exterior polygon ring)
                     SDO_ORDINATE_ARRAY(3, 3, 6, 3, 6, 5, 4, 5, 3, 3)));

-- Now insert an area of interest for Cola D. This is a
-- circle with a radius of 2. It is completely outside the
-- first three areas of interest.

insert into t_source_geo
values (4,
        'cola_d',
        SDO_GEOMETRY(2003, -- two-dimensional polygon
                     null, null, SDO_ELEM_INFO_ARRAY(1, 1003, 4), -- one circle
                     SDO_ORDINATE_ARRAY(8, 7, 10, 9, 8, 11)));