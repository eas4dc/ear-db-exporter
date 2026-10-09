-- EAR 5.0 - 6.x: ear-private v6.3.0 src/commands/ear_create_database.c (5.x lacks Signatures.cpu_util, GPU_signatures.GPU_gflops)
-- Verbatim MySQL DDL of EAR's edb_create (DBReportSigDetail=1, DBReportNodeDetail=1,
-- USE_GPUS, USE_LEARNING_APPS, no K8_SUPPORT), one column per line, plus the indexes
-- edb_create -r adds. Used to check the profile covers every column and by ear-db-synth.
CREATE TABLE IF NOT EXISTS Applications (
  job_id INT unsigned NOT NULL,
  step_id INT unsigned NOT NULL,
  local_id INT unsigned NOT NULL,
  node_id VARCHAR(64),
  signature_id BIGINT unsigned,
  power_signature_id INT unsigned,
  PRIMARY KEY(job_id, step_id, local_id, node_id)
);
CREATE TABLE IF NOT EXISTS Loops (
  event INT unsigned NOT NULL,
  size INT unsigned NOT NULL,
  level INT unsigned NOT NULL,
  job_id INT unsigned NOT NULL,
  step_id INT unsigned NOT NULL,
  local_id INT unsigned NOT NULL,
  node_id VARCHAR(64),
  total_iterations INT unsigned,
  signature_id BIGINT unsigned
);
CREATE TABLE IF NOT EXISTS Jobs (
  job_id INT unsigned NOT NULL,
  step_id INT unsigned NOT NULL,
  local_id INT unsigned NOT NULL,
  user_id VARCHAR(128),
  app_id VARCHAR(128),
  start_time INT NOT NULL,
  end_time INT NOT NULL,
  start_mpi_time INT NOT NULL,
  end_mpi_time INT NOT NULL,
  policy VARCHAR(256) NOT NULL,
  threshold FLOAT NOT NULL,
  procs INT unsigned NOT NULL,
  job_type SMALLINT unsigned NOT NULL,
  def_f INT unsigned,
  user_acc VARCHAR(256),
  user_group VARCHAR(256),
  e_tag VARCHAR(256),
  PRIMARY KEY(job_id, step_id, local_id)
);
CREATE TABLE IF NOT EXISTS Signatures (
  id BIGINT unsigned NOT NULL AUTO_INCREMENT,
  DC_power FLOAT,
  DRAM_power FLOAT,
  PCK_power FLOAT,
  EDP FLOAT,
  GBS FLOAT,
  IO_MBS FLOAT,
  TPI FLOAT,
  CPI FLOAT,
  Gflops FLOAT,
  time FLOAT,
  perc_MPI FLOAT,
  L1_misses BIGINT unsigned,
  L2_misses BIGINT unsigned,
  L3_misses BIGINT unsigned,
  FLOPS1 BIGINT unsigned,
  FLOPS2 BIGINT unsigned,
  FLOPS3 BIGINT unsigned,
  FLOPS4 BIGINT unsigned,
  FLOPS5 BIGINT unsigned,
  FLOPS6 BIGINT unsigned,
  FLOPS7 BIGINT unsigned,
  FLOPS8 BIGINT unsigned,
  instructions BIGINT unsigned,
  cycles BIGINT unsigned,
  avg_f INT unsigned,
  avg_imc_f INT unsigned,
  def_f INT unsigned,
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
  local_id INT unsigned NOT NULL,
  node_id VARCHAR(64),
  signature_id BIGINT unsigned,
  power_signature_id INT unsigned,
  PRIMARY KEY(job_id, step_id, local_id, node_id)
);
CREATE TABLE IF NOT EXISTS Learning_jobs (
  job_id INT unsigned NOT NULL,
  step_id INT unsigned NOT NULL,
  local_id INT unsigned NOT NULL,
  user_id VARCHAR(256),
  app_id VARCHAR(256),
  start_time INT NOT NULL,
  end_time INT NOT NULL,
  start_mpi_time INT NOT NULL,
  end_mpi_time INT NOT NULL,
  policy VARCHAR(256) NOT NULL,
  threshold FLOAT NOT NULL,
  procs INT unsigned NOT NULL,
  job_type SMALLINT unsigned NOT NULL,
  def_f INT unsigned,
  user_acc VARCHAR(256) NOT NULL,
  user_group VARCHAR(256),
  e_tag VARCHAR(256),
  PRIMARY KEY(job_id, step_id, local_id)
);
CREATE TABLE IF NOT EXISTS Learning_signatures (
  id BIGINT unsigned NOT NULL AUTO_INCREMENT,
  DC_power FLOAT,
  DRAM_power FLOAT,
  PCK_power FLOAT,
  EDP FLOAT,
  GBS FLOAT,
  IO_MBS FLOAT,
  TPI FLOAT,
  CPI FLOAT,
  Gflops FLOAT,
  time FLOAT,
  perc_MPI FLOAT,
  L1_misses BIGINT unsigned,
  L2_misses BIGINT unsigned,
  L3_misses BIGINT unsigned,
  FLOPS1 BIGINT unsigned,
  FLOPS2 BIGINT unsigned,
  FLOPS3 BIGINT unsigned,
  FLOPS4 BIGINT unsigned,
  FLOPS5 BIGINT unsigned,
  FLOPS6 BIGINT unsigned,
  FLOPS7 BIGINT unsigned,
  FLOPS8 BIGINT unsigned,
  instructions BIGINT unsigned,
  cycles BIGINT unsigned,
  avg_f INT unsigned,
  avg_imc_f INT unsigned,
  def_f INT unsigned,
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
CREATE INDEX idx_user_id ON Jobs (user_id);
