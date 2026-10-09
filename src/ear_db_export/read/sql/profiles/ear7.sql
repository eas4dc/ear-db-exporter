-- EAR 7: ear-private origin/v7priv src/commands/ear_create_database.c
-- Verbatim MySQL DDL of EAR's edb_create (DBReportSigDetail=1, DBReportNodeDetail=1,
-- USE_GPUS, USE_LEARNING_APPS, no K8_SUPPORT), one column per line, plus the indexes
-- edb_create -r adds. Used to check the profile covers every column and by ear-db-synth.
CREATE TABLE IF NOT EXISTS Applications (
  job_id INT unsigned NOT NULL,
  step_id INT unsigned NOT NULL,
  app_id INT unsigned NOT NULL,
  node_name VARCHAR(64),
  earl_signature_id BIGINT unsigned,
  eard_signature_id INT unsigned,
  PRIMARY KEY(job_id, step_id, app_id, node_name)
);
CREATE TABLE IF NOT EXISTS Loops (
  entry INT unsigned NOT NULL,
  size INT unsigned NOT NULL,
  level INT unsigned NOT NULL,
  job_id INT unsigned NOT NULL,
  step_id INT unsigned NOT NULL,
  app_id INT unsigned NOT NULL,
  node_name VARCHAR(64),
  timestamp INT unsigned,
  signature_id BIGINT unsigned
);
CREATE TABLE IF NOT EXISTS Jobs (
  job_id INT unsigned NOT NULL,
  step_id INT unsigned NOT NULL,
  app_id INT unsigned NOT NULL,
  user_name VARCHAR(128),
  user_acc VARCHAR(256),
  user_group VARCHAR(256),
  app_name VARCHAR(128),
  start_time INT NOT NULL,
  end_time INT NOT NULL,
  earl_start_time INT NOT NULL,
  earl_end_time INT NOT NULL,
  policy VARCHAR(256) NOT NULL,
  threshold FLOAT NOT NULL,
  num_procs INT unsigned NOT NULL,
  def_cpu_freq INT unsigned,
  def_gpu_freq INT unsigned,
  e_tag VARCHAR(256),
  PRIMARY KEY(job_id, step_id, app_id)
);
CREATE TABLE IF NOT EXISTS Signatures (
  id BIGINT unsigned NOT NULL AUTO_INCREMENT,
  node_power FLOAT,
  dram_power FLOAT,
  pck_power FLOAT,
  dram_bandwidth FLOAT,
  io_bandwidth FLOAT,
  TPI FLOAT,
  CPI FLOAT,
  cpu_gflops FLOAT,
  elapsed_time FLOAT,
  perc_MPI FLOAT,
  L1_misses BIGINT unsigned,
  L2_misses BIGINT unsigned,
  L3_misses BIGINT unsigned,
  sp64_ops BIGINT unsigned,
  sp128_ops BIGINT unsigned,
  sp256_ops BIGINT unsigned,
  sp512_ops BIGINT unsigned,
  dp64_ops BIGINT unsigned,
  dp128_ops BIGINT unsigned,
  dp256_ops BIGINT unsigned,
  dp512_ops BIGINT unsigned,
  instructions BIGINT unsigned,
  cycles BIGINT unsigned,
  avg_cpu_freq INT unsigned,
  avg_imc_freq INT unsigned,
  def_cpu_freq INT unsigned,
  cpu_util INT unsigned,
  min_GPU_sig_id BIGINT unsigned,
  max_GPU_sig_id BIGINT unsigned,
  PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS GPU_signatures (
  id BIGINT unsigned NOT NULL AUTO_INCREMENT,
  GPU_power FLOAT NOT NULL,
  GPU_freq INT unsigned NOT NULL,
  GPU_mem_freq INT unsigned NOT NULL,
  GPU_util INT unsigned NOT NULL,
  GPU_mem_util INT unsigned NOT NULL,
  GPU_gflops FLOAT NOT NULL,
  gr_engine_active FLOAT NOT NULL,
  sm_active FLOAT NOT NULL,
  sm_occupancy FLOAT unsigned NOT NULL,
  tensor_active FLOAT NOT NULL,
  dram_active FLOAT NOT NULL,
  fp64_active FLOAT NOT NULL,
  fp32_active FLOAT NOT NULL,
  fp16_active FLOAT NOT NULL,
  pcie_tx_bytes INT unsigned NOT NULL,
  pcie_rx_bytes INT unsigned NOT NULL,
  nvlink_tx_bytes INT unsigned NOT NULL,
  nvlink_rx_bytes INT unsigned NOT NULL,
  PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS Periodic_metrics (
  id BIGINT unsigned NOT NULL AUTO_INCREMENT,
  start_time INT NOT NULL,
  end_time INT NOT NULL,
  DC_energy INT unsigned NOT NULL,
  node_id VARCHAR(64) NOT NULL,
  job_id INT unsigned NOT NULL,
  step_id INT unsigned NOT NULL,
  avg_f INT,
  temp INT,
  DRAM_energy INT,
  PCK_energy INT,
  GPU_energy INT,
  PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS Power_signatures (
  id INT unsigned NOT NULL AUTO_INCREMENT,
  DC_power FLOAT NOT NULL,
  DRAM_power FLOAT NOT NULL,
  PCK_power FLOAT NOT NULL,
  EDP FLOAT NOT NULL,
  max_DC_power FLOAT NOT NULL,
  min_DC_power FLOAT NOT NULL,
  time FLOAT NOT NULL,
  avg_f INT unsigned NOT NULL,
  def_f INT unsigned NOT NULL,
  PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS Events (
  id INT unsigned NOT NULL AUTO_INCREMENT,
  timestamp INT NOT NULL,
  event_type INT NOT NULL,
  job_id INT unsigned NOT NULL,
  step_id INT unsigned NOT NULL,
  value INT NOT NULL,
  node_id VARCHAR(64),
  PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS Global_energy (
  energy_percent FLOAT,
  warning_level INT UNSIGNED NOT NULL,
  time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  inc_th FLOAT,
  p_state INT,
  GlobEnergyConsumedT1 INT UNSIGNED,
  GlobEnergyConsumedT2 INT UNSIGNED,
  GlobEnergyLimit INT UNSIGNED,
  GlobEnergyPeriodT1 INT UNSIGNED,
  GlobEnergyPeriodT2 INT UNSIGNED,
  GlobEnergyPolicy VARCHAR(64),
  PRIMARY KEY (time)
);
CREATE TABLE IF NOT EXISTS Learning_applications (
  job_id INT unsigned NOT NULL,
  step_id INT unsigned NOT NULL,
  app_id INT unsigned NOT NULL,
  node_name VARCHAR(64),
  earl_signature_id BIGINT unsigned,
  eard_signature_id INT unsigned,
  PRIMARY KEY(job_id, step_id, app_id, node_name)
);
CREATE TABLE IF NOT EXISTS Learning_jobs (
  job_id INT unsigned NOT NULL,
  step_id INT unsigned NOT NULL,
  app_id INT unsigned NOT NULL,
  user_name VARCHAR(256),
  user_acc VARCHAR(256) NOT NULL,
  user_group VARCHAR(256),
  app_name VARCHAR(256),
  start_time INT NOT NULL,
  end_time INT NOT NULL,
  earl_start_time INT NOT NULL,
  earl_end_time INT NOT NULL,
  policy VARCHAR(256) NOT NULL,
  threshold FLOAT NOT NULL,
  num_procs INT unsigned NOT NULL,
  def_cpu_freq INT unsigned,
  def_gpu_freq INT unsigned,
  e_tag VARCHAR(256),
  PRIMARY KEY(job_id, step_id, app_id)
);
CREATE TABLE IF NOT EXISTS Learning_signatures (
  id BIGINT unsigned NOT NULL AUTO_INCREMENT,
  node_power FLOAT,
  dram_power FLOAT,
  pck_power FLOAT,
  dram_bandwidth FLOAT,
  io_bandwidth FLOAT,
  TPI FLOAT,
  CPI FLOAT,
  cpu_gflops FLOAT,
  elapsed_time FLOAT,
  perc_MPI FLOAT,
  L1_misses BIGINT unsigned,
  L2_misses BIGINT unsigned,
  L3_misses BIGINT unsigned,
  sp64_ops BIGINT unsigned,
  sp128_ops BIGINT unsigned,
  sp256_ops BIGINT unsigned,
  sp512_ops BIGINT unsigned,
  dp64_ops BIGINT unsigned,
  dp128_ops BIGINT unsigned,
  dp256_ops BIGINT unsigned,
  dp512_ops BIGINT unsigned,
  instructions BIGINT unsigned,
  cycles BIGINT unsigned,
  avg_cpu_freq INT unsigned,
  avg_imc_freq INT unsigned,
  def_cpu_freq INT unsigned,
  cpu_util INT unsigned,
  min_GPU_sig_id BIGINT unsigned,
  max_GPU_sig_id BIGINT unsigned,
  PRIMARY KEY (id)
);
CREATE TABLE IF NOT EXISTS Periodic_aggregations (
  id INT unsigned NOT NULL AUTO_INCREMENT,
  start_time INT,
  end_time INT,
  DC_energy INT unsigned,
  eardbd_host VARCHAR(64),
  PRIMARY KEY(id)
);
CREATE INDEX idx_end_time ON Periodic_metrics (end_time);
CREATE INDEX idx_job_id ON Periodic_metrics (job_id);
CREATE INDEX idx_user_name ON Jobs (user_name);
