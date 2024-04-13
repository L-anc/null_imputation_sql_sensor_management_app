-- Notes for ER diagram: use formulas from slides, 
-- every symbol matters (even direction of arrow)


-- Use views for corrected

-- running database null corrections in python

-- keep track of background research in readme

-- trigger happens with views

-- can ease up on functions

-- Make diagrams:
-- Color views differently in ER diagram to distinguish between tables,
-- show cardinalities (annotate arrows by what I do to create the view),
-- include a legend to explain customizations to ER diagram.

-- Filter out all null columns as estimation techniques will not work on them

-- use regex to remove all quotes and set capital to lower case column names


-- clean up old tables
DROP TABLE IF EXISTS aggregate;
DROP TABLE IF EXISTS agg_blackhole;
DROP TABLE IF EXISTS sensors;
DROP TABLE IF EXISTS expeditions;
DROP TRIGGER IF EXISTS sens_sanitize;

CREATE TABLE expeditions (
    exped_id     VARCHAR(10),
    cost         NUMERIC(10, 2) NULL,
    PRIMARY KEY (exped_id)
);

CREATE TABLE sensors (
    -- unique serial number for individual sensor
    station       VARCHAR(10),
    -- ID number for expedition on which it was deployed
    exped_id      VARCHAR(10),
    -- Name of sensor producing company
    company       VARCHAR(50)   NOT NULL,
    eol_date      DATETIME      NOT NULL,
    PRIMARY KEY (station),
    FOREIGN KEY (exped_id) REFERENCES expeditions(exped_id)
);

-- Aggregate dataset of oceanic sensor data
CREATE TABLE aggregate (
    -- -- Numeric identifier for a specific dataset
    -- db              TINYINT     NOT NULL,
    -- Guaranteed data
    station         VARCHAR(10) NOT NULL,
    date            DATETIME    NOT NULL,
    latitude        FLOAT       NOT NULL,
    longitude       FLOAT       NOT NULL,
    -- Remaining data (possibly null)
    elevation       FLOAT,
    name            VARCHAR(20),
    imma_ver        TINYINT,
    attm_ct         TINYINT,
    time_ind        TINYINT,
    ll_ind          TINYINT,
    ship_course     TINYINT,
    ship_speed      TINYINT,
    id_ind          TINYINT,
    wind_dir_ind    TINYINT,
    wind_dir        INT,
    wind_spd_ind    TINYINT,
    wind_speed      INT,
    vv_ind          TINYINT,
    visibility      INT,
    pres_wx         INT,
    past_wx         TINYINT,
    sea_level_pres  INT,
    char_ppp        TINYINT,
    amt_pres_tend   INT,
    ind_for_temp    TINYINT,
    air_temp        INT,
    ind_for_wbt     TINYINT,
    wet_bulb_temp   INT,
    dew_pt_temp     INT,
    sst_mm          TINYINT,
    sea_surf_temp   INT,
    tot_cld_amt     TINYINT,
    low_cld_amt     TINYINT,
    low_cld_type    VARCHAR(10),
    cld_hgt         VARCHAR(1),
    mid_cld_type    VARCHAR(10),
    hi_cld_type     VARCHAR(10),
    wave_dir        INT,
    wave_period     TINYINT,
    wave_hgt        TINYINT,
    swell_dir       INT,
    swell_period    TINYINT,
    swell_hgt       TINYINT,
    ten_box_num     INT,
    one_box_num     INT,
    deck            INT,
    source_id       INT,
    platform_id     TINYINT,
    dup_status      TINYINT,
    dup_chk         TINYINT,
    night_day_flag  TINYINT,
    trim_flag       VARCHAR(10),
    ncdc_qc_flags   VARCHAR(20),
    source_exclusion_flag   TINYINT,
    ob_source       TINYINT,
    sta_wx_ind      TINYINT,
    past_wx2        TINYINT,
    dir_of_swell2   INT,
    per_of_swell2   TINYINT,
    hgt_of_swell2   TINYINT,
    thickness_of_i  TINYINT,
    concen_of_sea_ice   TINYINT,
    ice_of_land_origin  VARCHAR(10),
    ind_for_precip  TINYINT
);

CREATE TABLE agg_blackhole (
    -- -- Numeric identifier for a specific dataset
    -- db              TINYINT     NOT NULL,
    -- Guaranteed data
    station         VARCHAR(10) NOT NULL,
    date            DATETIME    NOT NULL,
    latitude        FLOAT       NOT NULL,
    longitude       FLOAT       NOT NULL,
    -- Remaining data (possibly null)
    elevation       FLOAT,
    name            VARCHAR(20),
    imma_ver        TINYINT,
    attm_ct         TINYINT,
    time_ind        TINYINT,
    ll_ind          TINYINT,
    ship_course     TINYINT,
    ship_speed      TINYINT,
    id_ind          TINYINT,
    wind_dir_ind    TINYINT,
    wind_dir        INT,
    wind_spd_ind    TINYINT,
    wind_speed      INT,
    vv_ind          TINYINT,
    visibility      INT,
    pres_wx         INT,
    past_wx         TINYINT,
    sea_level_pres  INT,
    char_ppp        TINYINT,
    amt_pres_tend   INT,
    ind_for_temp    TINYINT,
    air_temp        INT,
    ind_for_wbt     TINYINT,
    wet_bulb_temp   INT,
    dew_pt_temp     INT,
    sst_mm          TINYINT,
    sea_surf_temp   INT,
    tot_cld_amt     TINYINT,
    low_cld_amt     TINYINT,
    low_cld_type    VARCHAR(10),
    cld_hgt         VARCHAR(1),
    mid_cld_type    VARCHAR(10),
    hi_cld_type     VARCHAR(10),
    wave_dir        INT,
    wave_period     TINYINT,
    wave_hgt        TINYINT,
    swell_dir       INT,
    swell_period    TINYINT,
    swell_hgt       TINYINT,
    ten_box_num     INT,
    one_box_num     INT,
    deck            INT,
    source_id       INT,
    platform_id     TINYINT,
    dup_status      TINYINT,
    dup_chk         TINYINT,
    night_day_flag  TINYINT,
    trim_flag       VARCHAR(10),
    ncdc_qc_flags   VARCHAR(20),
    source_exclusion_flag   TINYINT,
    ob_source       TINYINT,
    sta_wx_ind      TINYINT,
    past_wx2        TINYINT,
    dir_of_swell2   INT,
    per_of_swell2   TINYINT,
    hgt_of_swell2   TINYINT,
    thickness_of_i  TINYINT,
    concen_of_sea_ice   TINYINT,
    ice_of_land_origin  VARCHAR(10),
    ind_for_precip  TINYINT
) ENGINE = BLACKHOLE;

-- Trigger to discount sensor data past the sensor end of life date
delimiter !
CREATE TRIGGER sens_sanitize BEFORE INSERT ON agg_blackhole
FOR EACH ROW BEGIN
    IF NOT EXISTS(
            SELECT 1
            FROM sensors AS s
            WHERE NEW.station = s.station AND NEW.date >= eol_date)
    THEN INSERT INTO aggregate VALUES 
        (NEW.station               ,
        NEW.date                  ,
        NEW.latitude              ,
        NEW.longitude             ,
        NEW.elevation             ,
        NEW.name                  ,
        NEW.imma_ver              ,
        NEW.attm_ct               ,
        NEW.time_ind              ,
        NEW.ll_ind                ,
        NEW.ship_course           ,
        NEW.ship_speed            ,
        NEW.id_ind                ,
        NEW.wind_dir_ind          ,
        NEW.wind_dir              ,
        NEW.wind_spd_ind          ,
        NEW.wind_speed            ,
        NEW.vv_ind                ,
        NEW.visibility            ,
        NEW.pres_wx               ,
        NEW.past_wx               ,
        NEW.sea_level_pres        ,
        NEW.char_ppp              ,
        NEW.amt_pres_tend         ,
        NEW.ind_for_temp          ,
        NEW.air_temp              ,
        NEW.ind_for_wbt           ,
        NEW.wet_bulb_temp         ,
        NEW.dew_pt_temp           ,
        NEW.sst_mm                ,
        NEW.sea_surf_temp         ,
        NEW.tot_cld_amt           ,
        NEW.low_cld_amt           ,
        NEW.low_cld_type          ,
        NEW.cld_hgt               ,
        NEW.mid_cld_type          ,
        NEW.hi_cld_type           ,
        NEW.wave_dir              ,
        NEW.wave_period           ,
        NEW.wave_hgt              ,
        NEW.swell_dir             ,
        NEW.swell_period          ,
        NEW.swell_hgt             ,
        NEW.ten_box_num           ,
        NEW.one_box_num           ,
        NEW.deck                  ,
        NEW.source_id             ,
        NEW.platform_id           ,
        NEW.dup_status            ,
        NEW.dup_chk               ,
        NEW.night_day_flag        ,
        NEW.trim_flag             ,
        NEW.ncdc_qc_flags         ,
        NEW.source_exclusion_flag ,
        NEW.ob_source             ,
        NEW.sta_wx_ind            ,
        NEW.past_wx2              ,
        NEW.dir_of_swell2         ,
        NEW.per_of_swell2         ,
        NEW.hgt_of_swell2         ,
        NEW.thickness_of_i        ,
        NEW.concen_of_sea_ice     ,
        NEW.ice_of_land_origin    ,
        NEW.ind_for_precip);
    END IF;
END !
delimiter ;

-- Dummy data for expeditions
INSERT INTO expeditions VALUES
    ('NA2257', 60000),
    ('AN665', 10000),
    ('WP778', 50000),
    ('NP7', 2000),
    ('BS12', 10000);

-- Dummy data for sensors
INSERT INTO sensors VALUES
    ('2802017', 'NA2257', 'Hamilton Industries', '2024-01-01T00:00:00'),
    ('BAXC1', 'NA2257', 'Hamilton Industries', '2024-01-01T00:00:00'),
    ('AAMC1', 'BS12', 'General Electric', '2024-01-01T00:06:00'),
    ('PORO3', 'NP7', 'VIA', '2024-01-31T23:06:00'),
    ('PXOC1', 'WP778', 'VIA', '2024-01-17T13:24:00');

-- Load the data from the 3 csv databases into aggregate

-- LOAD DATA LOCAL INFILE 'test.csv' INTO TABLE aggregate
-- FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '40_-120_30_-110.csv' INTO TABLE agg_blackhole
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '40_-130_30_-120.csv' INTO TABLE agg_blackhole
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE '50_-130_40_-120.csv' INTO TABLE agg_blackhole
FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

-- Replace default empty string values with NULLs

UPDATE aggregate
SET 
    elevation             = NULLIF(elevation             , ''),  
    name                  = NULLIF(name                  , ''),       
    imma_ver              = NULLIF(imma_ver              , ''),    
    attm_ct               = NULLIF(attm_ct               , ''), 
    time_ind              = NULLIF(time_ind              , ''),    
    ll_ind                = NULLIF(ll_ind                , ''),     
    ship_course           = NULLIF(ship_course           , ''),    
    ship_speed            = NULLIF(ship_speed            , ''),     
    id_ind                = NULLIF(id_ind                , ''),  
    wind_dir_ind          = NULLIF(wind_dir_ind          , ''),   
    wind_dir              = NULLIF(wind_dir              , ''),    
    wind_spd_ind          = NULLIF(wind_spd_ind          , ''),       
    wind_speed            = NULLIF(wind_speed            , ''),     
    vv_ind                = NULLIF(vv_ind                , ''),    
    visibility            = NULLIF(visibility            , ''),     
    pres_wx               = NULLIF(pres_wx               , ''),    
    past_wx               = NULLIF(past_wx               , ''),    
    sea_level_pres        = NULLIF(sea_level_pres        , ''),    
    char_ppp              = NULLIF(char_ppp              , ''),    
    amt_pres_tend         = NULLIF(amt_pres_tend         , ''),          
    ind_for_temp          = NULLIF(ind_for_temp          , ''),         
    air_temp              = NULLIF(air_temp              , ''),        
    ind_for_wbt           = NULLIF(ind_for_wbt           , ''),      
    wet_bulb_temp         = NULLIF(wet_bulb_temp         , ''),     
    dew_pt_temp           = NULLIF(dew_pt_temp           , ''),    
    sst_mm                = NULLIF(sst_mm                , ''),     
    sea_surf_temp         = NULLIF(sea_surf_temp         , ''),   
    tot_cld_amt           = NULLIF(tot_cld_amt           , ''),     
    low_cld_amt           = NULLIF(low_cld_amt           , ''),     
    low_cld_type          = NULLIF(low_cld_type          , ''),   
    cld_hgt               = NULLIF(cld_hgt               , ''),     
    mid_cld_type          = NULLIF(mid_cld_type          , ''),     
    hi_cld_type           = NULLIF(hi_cld_type           , ''),    
    wave_dir              = NULLIF(wave_dir              , ''),     
    wave_period           = NULLIF(wave_period           , ''),     
    wave_hgt              = NULLIF(wave_hgt              , ''),     
    swell_dir             = NULLIF(swell_dir             , ''),    
    swell_period          = NULLIF(swell_period          , ''),   
    swell_hgt             = NULLIF(swell_hgt             , ''),    
    ten_box_num           = NULLIF(ten_box_num           , ''),    
    one_box_num           = NULLIF(one_box_num           , ''),     
    deck                  = NULLIF(deck                  , ''),    
    source_id             = NULLIF(source_id             , ''),     
    platform_id           = NULLIF(platform_id           , ''),        
    dup_status            = NULLIF(dup_status            , ''),     
    dup_chk               = NULLIF(dup_chk               , ''),     
    night_day_flag        = NULLIF(night_day_flag        , ''),        
    trim_flag             = NULLIF(trim_flag             , ''),     
    ncdc_qc_flags         = NULLIF(ncdc_qc_flags         , ''),                
    source_exclusion_flag = NULLIF(source_exclusion_flag , ''),        
    ob_source             = NULLIF(ob_source             , ''),       
    sta_wx_ind            = NULLIF(sta_wx_ind            , ''),   
    past_wx2              = NULLIF(past_wx2              , ''),       
    dir_of_swell2         = NULLIF(dir_of_swell2         , ''),       
    per_of_swell2         = NULLIF(per_of_swell2         , ''),       
    hgt_of_swell2         = NULLIF(hgt_of_swell2         , ''),    
    thickness_of_i        = NULLIF(thickness_of_i        , ''),    
    concen_of_sea_ice     = NULLIF(concen_of_sea_ice     , ''),        
    ice_of_land_origin    = NULLIF(ice_of_land_origin    , ''),        
    ind_for_precip        = NULLIF(ind_for_precip        , ''); 
