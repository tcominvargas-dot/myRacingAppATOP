CREATE TABLE `app_config_box_interval` (
   `low_time_seconds` int NOT NULL,
   `high_time_seconds` int NOT NULL,
   PRIMARY KEY (`low_time_seconds`, `high_time_seconds`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ;
CREATE TABLE `app_config_lap_interval` (
   `start_lap` int NOT NULL,
   `end_lap` int NOT NULL,
   `min_time_seconds` decimal(10,3) NOT NULL,
   `max_time_seconds` decimal(10,3) NOT NULL,
   PRIMARY KEY (`start_lap`, `end_lap`)
 ) ENGINE=InnoDB DEFAULT CHARSE...
CREATE TABLE `app_config` (
   `id` tinyint unsigned NOT NULL,
   `api_token` varchar(128) NOT NULL,
   `race_id` int NOT NULL,
   `last_used` datetime,
   `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP DEFAULT_GENERATED,
   PRIMARY KEY (`id`)
 ) ENGI...
CREATE TABLE `competitor_laps` (
   `id` bigint NOT NULL auto_increment,
   `race_id` int NOT NULL,
   `racer_id` int NOT NULL,
   `lap_number` int NOT NULL,
   `position` int,
   `lap_time` varchar(20),
   `flag_status` varchar(50),
   `total_time` varchar(20),...
CREATE TABLE `competitors` (
   `racer_id` int NOT NULL,
   `race_id` int NOT NULL,
   `number` varchar(20),
   `transponder` varchar(50),
   `first_name` varchar(100),
   `last_name` varchar(100),
   `nationality` varchar(100),
   `additional_data` varchar(100)...
CREATE TABLE `update_group_2min` (
   `racer_id` int NOT NULL,
   `last_update` datetime,
   PRIMARY KEY (`racer_id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ;
CREATE TABLE `update_group_4min` (
   `racer_id` int NOT NULL,
   `last_update` datetime,
   PRIMARY KEY (`racer_id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ;
CREATE TABLE `update_group_rest` (
   `racer_id` int NOT NULL,
   `last_update` datetime,
   PRIMARY KEY (`racer_id`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ;