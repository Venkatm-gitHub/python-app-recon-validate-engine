###################################### logger.py ######################################
import logging
import os

def setup_logging():
    log_file = os.getenv("RULES_ENGINE_LOG", "rules_engine.log")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


###################################### models.py ######################################
from dataclasses import dataclass
from typing import Dict, List, Any, Optionalda

@dataclass
class Rule:
    rule_id: str
    rule_name: str
    rule_type: str
    rule_status: str
    module: str
    stage: str
    package: str
    slg: str
    program_type: str
    program_code: str
    created_dt: str = ""
    created_by: str = ""
    modified_dt: str = ""
    modified_by: str = ""

@dataclass
class RuleExecutionStatus:
    rule_run_id: str
    rule_id: str
    rule_name: str
    rule_type: str
    rule_status: str
    module: str
    stage: str
    package: str
    slg: str
    program_type: str
    program_code: str
    rule_execution_status: str
    execution_date: str = ""
    processing_date: str = ""
    exception_applied: str = "No"
    action: str = ""
    workflow_status: str = ""

@dataclass
class TableConfig:
    logical_name: str
    path: str
    kind: Dict[str, str]
    schema: Dict[str, List[Dict[str, str]]]

@dataclass
class RuleSpec:
    rule: Rule
    data_config: Dict[str, Any]
    dictionary_config: Dict[str, Any]
    as_of_date: str
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, str]]:
        for table in self.dictionary_config.get('tables', []):
            if table['logicalName'] == table_name:
                return table.get('schema', {}).get('RAW', [])
        return []


###################################### interfaces.py ######################################
from abc import ABC, abstractmethod
from typing import List, Dict, Any
from models import Rule, RuleExecutionStatus, RuleSpec

class IRuleService(ABC):
    @abstractmethod
    def get_active_rules_with_feeds(self, model: str, stage: str, package: str, feed_id: str = None) -> List[Rule]:
        pass

class IExecutionStatusService(ABC):
    @abstractmethod
    def insert_execution_status(self, status: RuleExecutionStatus):
        pass
    @abstractmethod
    def get_execution_status(self, rule_ids: List[str]) -> List[tuple]:
        pass

class IFeedMappingService(ABC):
    @abstractmethod
    def rule_has_feed_mapping(self, rule_id: str) -> bool:
        pass

class IDataFrameManager(ABC):
    @abstractmethod
    def create_dataframes(self, spark_session, rule_spec: RuleSpec) -> Dict[str, Any]:
        pass

class IRuleExecutor(ABC):
    @abstractmethod
    def execute_rule(self, spark_session, rule_spec: RuleSpec, dataframes: Dict[str, Any]) -> Any:
        pass

class IResultExporter(ABC):
    @abstractmethod
    def export_results(self, result_df, rule_run_id: str, rule_id: str, hdfs_output_path: str):
        pass

class ISparkJobManager(ABC):
    @abstractmethod
    def submit_spark_job(self, rule_spec: RuleSpec, run_id: str) -> str:
        pass

class IPollingEngine(ABC):
    @abstractmethod
    def poll_for_exceptions(self, rule_ids: List[str]) -> Dict[str, bool]:
        pass

class IValidationEngine(ABC):
    @abstractmethod
    def validate_rules(self, rule_ids: List[str]) -> bool:
        pass


##################################### database_service.py #################################
###################################### database_service.py ######################################
from service import DatabaseService
from exceptions import DatabaseException
from models import Rule, RuleExecutionStatus
from interfaces import IRuleService, IExecutionStatusService, IFeedMappingService
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class BaseDatabaseService:
    def __init__(self):
        self.service = DatabaseService()

    def execute_query(self, query: str, params: dict) -> list:
        with self.service.create_session('oracle') as session:
            results, _ = session.reader.read_as_tuple(query, params)
            return results

class RuleService(BaseDatabaseService, IRuleService):
    def get_active_rules_with_feeds(self, model: str, stage: str, package: str, feed_id: str = None) -> List[Rule]:
        if feed_id:
            # Query for rules with feed mapping
            mapped_query = """
            SELECT DISTINCT r.RULE_ID, r.RULE_NAME, r.RULE_TYPE, r.RULE_STATUS, 
                   r.MODULE, r.STAGE, r.PACKAGE, r.SLG, r.PROGRAM_TYPE, r.PROGRAM_CODE,
                   r.CREATED_DT, r.CREATED_BY, r.MODIFIED_DT, r.MODIFIED_BY
            FROM TB_RULES_ENGINE_MST r
            INNER JOIN TB_RULES_FEED_CONFIG_MAP f ON r.RULE_ID = f.RULE_ID
            WHERE f.FEED_ID = :feed_id 
            AND f.RULE_APPLICABLE_STATUS = 'Yes'
            AND r.MODULE = :model
            AND r.STAGE = :stage
            AND r.PACKAGE = :package
            AND r.RULE_STATUS IN ('Active', 'Passive')
            """
            
            # Query for rules without any feed mapping
            unmapped_query = """
            SELECT DISTINCT r.RULE_ID, r.RULE_NAME, r.RULE_TYPE, r.RULE_STATUS, 
                   r.MODULE, r.STAGE, r.PACKAGE, r.SLG, r.PROGRAM_TYPE, r.PROGRAM_CODE,
                   r.CREATED_DT, r.CREATED_BY, r.MODIFIED_DT, r.MODIFIED_BY
            FROM TB_RULES_ENGINE_MST r
            WHERE NOT EXISTS (
                SELECT 1 
                FROM TB_RULES_FEED_CONFIG_MAP f 
                WHERE f.RULE_ID = r.RULE_ID
            )
            AND r.MODULE = :model
            AND r.STAGE = :stage
            AND r.PACKAGE = :package
            AND r.RULE_STATUS IN ('Active', 'Passive')
            """
            
            # Combine both queries with UNION
            query = f"({mapped_query}) UNION ({unmapped_query})"
            params = {
                'feed_id': feed_id, 
                'model': model, 
                'stage': stage, 
                'package': package
            }
        else:
            query = """
            SELECT DISTINCT r.RULE_ID, r.RULE_NAME, r.RULE_TYPE, r.RULE_STATUS, 
                   r.MODULE, r.STAGE, r.PACKAGE, r.SLG, r.PROGRAM_TYPE, r.PROGRAM_CODE,
                   r.CREATED_DT, r.CREATED_BY, r.MODIFIED_DT, r.MODIFIED_BY
            FROM TB_RULES_ENGINE_MST r
            WHERE r.MODULE = :model
            AND r.STAGE = :stage
            AND r.PACKAGE = :package
            AND r.RULE_STATUS IN ('Active', 'Passive')
            """
            params = {'model': model, 'stage': stage, 'package': package}

        try:
            results = self.execute_query(query, params)
            return self._map_results_to_rules(results)
        except Exception as e:
            logger.error(f"Failed to get rules: {e}")
            raise

    def _map_results_to_rules(self, results) -> List[Rule]:
        rules = []
        for row in results:
            rule = Rule(
                rule_id=row[0], rule_name=row[1], rule_type=row[2], rule_status=row[3],
                module=row[4], stage=row[5], package=row[6], slg=row[7],
                program_type=row[8], program_code=row[9],
                created_dt=str(row[10]) if row[10] else "",
                created_by=row[11] if row[11] else "",
                modified_dt=str(row[12]) if row[12] else "",
                modified_by=row[13] if row[13] else ""
            )
            rules.append(rule)
        return rules

class ExecutionStatusService(BaseDatabaseService, IExecutionStatusService):
    def insert_execution_status(self, status: RuleExecutionStatus):
        query = """
        INSERT INTO TB_RULES_EXECUTION_STATUS
        (RULE_RUN_ID, RULE_ID, RULE_NAME, RULE_TYPE, RULE_STATUS, MODULE, STAGE, PACKAGE, SLG, 
         PROGRAM_TYPE, PROGRAM_CODE, RULE_EXECUTION_STATUS, EXECUTION_DATE, PROCESSING_DATE, 
         EXCEPTION_APPLIED, ACTION, WORKFLOW_STATUS)
        VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17)
        """
        
        params = (
            status.rule_run_id, status.rule_id, status.rule_name,
            status.rule_type, status.rule_status, status.module,
            status.stage, status.package, status.slg,
            status.program_type, status.program_code,
            status.rule_execution_status, status.execution_date,
            status.processing_date, status.exception_applied,
            status.action, status.workflow_status
        )
        
        try:
            with self.service.create_session('oracle') as session:
                session.writer.execute_write(query, params)
        except Exception as e:
            logger.error(f"Failed to insert execution status: {e}")
            raise

    def get_execution_status(self, rule_ids: List[str]) -> List[tuple]:
        if not rule_ids: return []
        placeholders = ', '.join([f':id_{i}' for i in range(len(rule_ids))])
        
        query = f"""
        SELECT s.RULE_ID, s.RULE_EXECUTION_STATUS, s.EXCEPTION_APPLIED, s.RULE_STATUS
        FROM (
            SELECT RULE_ID, RULE_EXECUTION_STATUS, EXCEPTION_APPLIED, RULE_STATUS,
                   ROW_NUMBER() OVER (PARTITION BY RULE_ID ORDER BY CREATED_DT DESC) as rn
            FROM TB_RULES_EXECUTION_STATUS
            WHERE RULE_ID IN ({placeholders})
        ) s
        WHERE s.rn = 1
        """
        
        params = {f'id_{i}': rule_id for i, rule_id in enumerate(rule_ids)}
        
        try:
            return self.execute_query(query, params)
        except Exception as e:
            logger.error(f"Failed to get execution status: {e}")
            raise

class FeedMappingService(BaseDatabaseService, IFeedMappingService):
    def rule_has_feed_mapping(self, rule_id: str) -> bool:
        query = """
        SELECT COUNT(*) 
        FROM TB_RULES_FEED_CONFIG_MAP 
        WHERE RULE_ID = :rule_id 
        AND RULE_APPLICABLE_STATUS = 'Yes'
        """
        params = {'rule_id': rule_id}
        try:
            results = self.execute_query(query, params)
            return results[0][0] > 0 if results else False
        except Exception as e:
            logger.error(f"Failed to check feed mapping: {e}")
            return False


######################################## rule_configuration.py ####################################
import json
import logging
from models import RuleSpec

logger = logging.getLogger(__name__)

class RuleConfiguration:
    def __init__(self):
        self.data_config = self._load_config("data_config.json")
        self.dictionary_config = self._load_config("dictionary_config.json")

    def _load_config(self, file_path: str) -> dict:
        try:
            with open(file_path) as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"Config file not found: {file_path}")
            raise
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in config: {file_path}")
            raise

    def create_rule_spec(self, rule: Rule, as_of_date: str) -> RuleSpec:
        return RuleSpec(
            rule=rule,
            data_config=self.data_config,
            dictionary_config=self.dictionary_config,
            as_of_date=as_of_date
        )


######################################## dataframe_manager.py ####################################
import logging
from typing import Dict, Any
from interfaces import IDataFrameManager
from models import RuleSpec

logger = logging.getLogger(__name__)

class DataFrameManager(IDataFrameManager):
    def create_dataframes(self, spark_session, rule_spec: RuleSpec) -> Dict[str, Any]:
        dataframes = {}
        for table in rule_spec.data_config.get('tables', []):
            logical_name = table['logicalName']
            path = table['path']
            file_format = table['kind'].get('RAW', 'PARQUET').upper()
            schema_info = rule_spec.get_table_schema(logical_name)
            
            try:
                df = self._read_dataframe(spark_session, file_format, path)
                df = self._apply_schema_casting(df, schema_info)
                dataframes[logical_name] = df
                logger.info(f"Created DataFrame for {logical_name}")
            except Exception as e:
                logger.error(f"DataFrame creation failed: {e}")
                raise
        return dataframes

    def _read_dataframe(self, spark_session, file_format, path):
        if file_format == 'PARQUET':
            return spark_session.read.parquet(path)
        elif file_format == 'DELTA':
            return spark_session.read.format("delta").load(path)
        elif file_format == 'CSV':
            return spark_session.read.option("header", "true").csv(path)
        else:
            return spark_session.read.format(file_format.lower()).load(path)

    def _apply_schema_casting(self, df, schema_info: list):
        from pyspark.sql.functions import col
        from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType
        
        type_mapping = {
            'string': StringType(),
            'integer': IntegerType(),
            'double': DoubleType(),
            'date': DateType()
        }
        
        for column_info in schema_info:
            column_name = column_info.get('name')
            data_type = column_info.get('dataType', 'string').lower()
            
            if column_name in df.columns and data_type in type_mapping:
                df = df.withColumn(column_name, col(column_name).cast(type_mapping[data_type]))
        return df


######################################## rule_executor.py ######################################
import logging
from typing import Dict, Any
from interfaces import IRuleExecutor
from models import RuleSpec

logger = logging.getLogger(__name__)

class RuleExecutor(IRuleExecutor):
    def execute_rule(self, spark_session, rule_spec: RuleSpec, dataframes: Dict[str, Any]) -> Any:
        rule = rule_spec.rule
        
        if rule.program_type.upper() == 'SQL':
            return self._execute_sql_rule(spark_session, rule, dataframes, rule_spec)
        elif rule.program_type.upper() == 'PYTHON':
            return self._execute_python_dsl_rule(spark_session, rule_spec, dataframes)
        else:
            raise ValueError(f"Unsupported program type: {rule.program_type}")

    def _execute_sql_rule(self, spark_session, rule, dataframes: Dict[str, Any], rule_spec: RuleSpec):
        for logical_name, df in dataframes.items():
            df.createOrReplaceTempView(logical_name)
            logger.info(f"Created temp view: {logical_name}")
        
        try:
            with open(rule.program_code, 'r') as f:
                sql_query = f.read().replace('${as_of_date}', rule_spec.as_of_date)
            return spark_session.sql(sql_query)
        except Exception as e:
            logger.error(f"SQL rule execution failed: {rule.rule_id} - {e}")
            raise

    def _execute_python_dsl_rule(self, spark_session, rule_spec: RuleSpec, dataframes: Dict[str, Any]):
        rule = rule_spec.rule
        try:
            exec_context = {
                'spark': spark_session,
                'dataframes': dataframes,
                'as_of_date': rule_spec.as_of_date,
                'rule_id': rule.rule_id
            }
            with open(rule.program_code, 'r') as f:
                exec(f.read(), exec_context)
            
            if 'result_df' not in exec_context:
                raise ValueError("Python rule must create 'result_df' variable")
            return exec_context['result_df']
        except Exception as e:
            logger.error(f"Python DSL rule execution failed: {rule.rule_id} - {e}")
            raise


######################################## result_exporter.py ####################################
import subprocess
import logging
import os
from interfaces import IResultExporter

logger = logging.getLogger(__name__)

class ResultExporter(IResultExporter):
    def export_results(self, result_df, rule_run_id: str, rule_id: str, hdfs_output_path: str):
        try:
            output_path = f"{hdfs_output_path}/{rule_id}/{rule_run_id}"
            result_df.write.mode("overwrite").parquet(output_path)
            logger.info(f"Results written to HDFS: {output_path}")
            
            sqoop_cmd = [
                "sqoop", "export",
                "--connect", os.getenv("ORACLE_JDBC_URL"),
                "--username", os.getenv("ORACLE_USERNAME"),
                "--password", os.getenv("ORACLE_PASSWORD"),
                "--table", "TB_RULES_EXECUTION_RESULT_DATA",
                "--export-dir", output_path,
                "--input-fields-terminated-by", "\t"
            ]
            subprocess.run(sqoop_cmd, check=True)
            logger.info(f"Sqoop export completed for {rule_id}")
        except Exception as e:
            logger.error(f"Result export failed: {e}")
            raise


######################################## polling_engine.py ####################################
import time
import logging
from datetime import datetime
from typing import List, Dict
from interfaces import IPollingEngine, IExecutionStatusService, IFeedMappingService

logger = logging.getLogger(__name__)

class PollingEngine(IPollingEngine):
    def __init__(self, status_service: IExecutionStatusService, feed_service: IFeedMappingService):
        self.status_service = status_service
        self.feed_service = feed_service

    def poll_for_exceptions(self, rule_ids: List[str]) -> Dict[str, bool]:
        logger.info(f"Starting exception polling for {len(rule_ids)} rules")
        start_time = datetime.now()
        results = {rule_id: False for rule_id in rule_ids}
        
        for _ in range(60):  # 60 attempts * 1 min = 1 hour
            if self._check_exceptions_applied(rule_ids, results):
                return results
            time.sleep(60)
        
        logger.warning("Timeout waiting for exceptions")
        return results

    def _check_exceptions_applied(self, rule_ids, results):
        status_records = self.status_service.get_execution_status(rule_ids)
        remaining = []
        
        for record in status_records:
            rule_id, _, exception_applied, _ = record
            if exception_applied == "Yes":
                results[rule_id] = True
            else:
                remaining.append(rule_id)
        
        rule_ids[:] = remaining  # Update original list
        return not remaining


######################################## validation_engine.py ####################################
import logging
from typing import List, Dict
from interfaces import IValidationEngine, IFeedMappingService, IExecutionStatusService
from polling_engine import PollingEngine

logger = logging.getLogger(__name__)

class ValidationEngine(IValidationEngine):
    def __init__(self, status_service: IExecutionStatusService, feed_service: IFeedMappingService):
        self.status_service = status_service
        self.feed_service = feed_service
        self.polling_engine = PollingEngine(status_service, feed_service)

    def validate_rules(self, rule_ids: List[str]) -> bool:
        logger.info("Starting rules validation")
        status_records = self.status_service.get_execution_status(rule_ids)
        if not status_records: 
            logger.info("No execution status records found")
            return True
        
        rule_status_map, rules_to_poll = self._categorize_rules(status_records)
        
        if rules_to_poll:
            poll_results = self.polling_engine.poll_for_exceptions(rules_to_poll)
            rule_status_map.update(poll_results)
        
        return self._validate_final_status(rule_ids, rule_status_map)

    def _categorize_rules(self, status_records):
        rule_status_map = {}
        rules_to_poll = []
        
        for record in status_records:
            rule_id, exec_status, exception_applied, rule_status = record
            
            if exec_status == "Success":
                rule_status_map[rule_id] = True
            elif rule_status == "Passive":
                rule_status_map[rule_id] = True
            elif rule_status == "Active" and exec_status != "Success":
                if self.feed_service.rule_has_feed_mapping(rule_id):
                    rules_to_poll.append(rule_id)
                else:
                    rule_status_map[rule_id] = True
        return rule_status_map, rules_to_poll

    def _validate_final_status(self, rule_ids, status_map):
        all_ok = all(status_map.get(rid, False) for rid in rule_ids)
        logger.info(f"Validation {'succeeded' if all_ok else 'failed'}")
        return all_ok


######################################## rule_processor.py ######################################
import concurrent.futures
import logging
import time
import uuid
from datetime import datetime
from typing import List
from models import RuleSpec, RuleExecutionStatus
from interfaces import IExecutionStatusService, ISparkJobManager

logger = logging.getLogger(__name__)

class RuleProcessor:
    def __init__(self, status_service: IExecutionStatusService, spark_job_manager: ISparkJobManager):
        self.status_service = status_service
        self.spark_job_manager = spark_job_manager

    def process_rule(self, rule_spec: RuleSpec) -> bool:
        rule = rule_spec.rule
        run_id = self._generate_run_id()
        self._log_rule_start(rule, run_id)
        
        try:
            self.spark_job_manager.submit_spark_job(rule_spec, run_id)
            return True
        except Exception as e:
            self._log_rule_failure(rule, run_id, e)
            return False

    def _generate_run_id(self):
        return f"{uuid.uuid4().hex[:10]}_{int(time.time())}"

    def _log_rule_start(self, rule, run_id):
        logger.info(f"Processing rule: {rule.rule_id} with run_id: {run_id}")
        start_status = RuleExecutionStatus(
            rule_run_id=run_id,
            rule_id=rule.rule_id,
            rule_name=rule.rule_name,
            rule_type=rule.rule_type,
            rule_status=rule.rule_status,
            module=rule.module,
            stage=rule.stage,
            package=rule.package,
            slg=rule.slg,
            program_type=rule.program_type,
            program_code=rule.program_code,
            rule_execution_status="Started",
            execution_date=datetime.now().strftime("%Y-%m-%d"),
            processing_date=datetime.now().strftime("%Y-%m-%d"),
            workflow_status="In Progress"
        )
        self.status_service.insert_execution_status(start_status)

    def _log_rule_failure(self, rule, run_id, error):
        logger.error(f"Rule processing failed: {rule.rule_id} - {error}")
        failure_status = RuleExecutionStatus(
            rule_run_id=run_id,
            rule_id=rule.rule_id,
            rule_name=rule.rule_name,
            rule_type=rule.rule_type,
            rule_status=rule.rule_status,
            module=rule.module,
            stage=rule.stage,
            package=rule.package,
            slg=rule.slg,
            program_type=rule.program_type,
            program_code=rule.program_code,
            rule_execution_status="Failure",
            execution_date=datetime.now().strftime("%Y-%m-%d"),
            processing_date=datetime.now().strftime("%Y-%m-%d"),
            workflow_status="Failed"
        )
        self.status_service.insert_execution_status(failure_status)

    def process_rules_parallel(self, rule_specs: List[RuleSpec]) -> bool:
        logger.info(f"Processing {len(rule_specs)} rules in parallel")
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(self.process_rule, rs): rs.rule.rule_id for rs in rule_specs}
            success_count = 0
            
            for future in concurrent.futures.as_completed(futures):
                rule_id = futures[future]
                try:
                    if future.result():
                        success_count += 1
                except Exception as e:
                    logger.error(f"Rule processing error: {rule_id} - {e}")
        
        success = success_count == len(rule_specs)
        logger.info(f"Rules processed: {success_count}/{len(rule_specs)} succeeded")
        return success


######################################## spark_job_manager.py ####################################
import subprocess
import json
import logging
from interfaces import ISparkJobManager
from models import RuleSpec

logger = logging.getLogger(__name__)

class SparkJobManager(ISparkJobManager):
    def submit_spark_job(self, rule_spec: RuleSpec, run_id: str) -> str:
        rule = rule_spec.rule
        cmd = [
            "spark-submit",
            "--master", "yarn",
            "--deploy-mode", "cluster",
            "--name", f"Rule_{rule.rule_id}_{run_id}",
            "spark_driver.py",
            "--run_id", run_id,
            "--rule_spec", json.dumps({
                'rule': rule.__dict__,
                'data_config': rule_spec.data_config,
                'dictionary_config': rule_spec.dictionary_config,
                'as_of_date': rule_spec.as_of_date
            })
        ]
        try:
            subprocess.run(cmd, check=True)
            logger.info(f"Spark job submitted: {run_id}")
            return run_id
        except subprocess.CalledProcessError as e:
            logger.error(f"Spark submission failed: {e}")
            raise


######################################## spark_driver.py ########################################
import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Centralized logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_arguments():
    parser = argparse.ArgumentParser(description='Spark Rule Driver')
    parser.add_argument('--run_id', required=True)
    parser.add_argument('--rule_spec', required=True, help='JSON rule specification')
    return parser.parse_args()

def create_spark_session(run_id: str):
    return SparkSession.builder.appName(f"RuleExecution_{run_id}").getOrCreate()

def parse_rule_spec(rule_spec_json: str):
    return json.loads(rule_spec_json)

def create_dataframes(spark, rule_spec_data):
    from dataframe_manager import DataFrameManager
    from models import RuleSpec
    
    # Create dummy RuleSpec
    class TempRuleSpec:
        def __init__(self, data_config, dict_config, as_of_date):
            self.data_config = data_config
            self.dictionary_config = dict_config
            self.as_of_date = as_of_date
        def get_table_schema(self, table_name):
            for table in self.dictionary_config.get('tables', []):
                if table['logicalName'] == table_name:
                    return table.get('schema', {}).get('RAW', [])
            return []
    
    temp_spec = TempRuleSpec(
        rule_spec_data['data_config'],
        rule_spec_data['dictionary_config'],
        rule_spec_data['as_of_date']
    )
    df_manager = DataFrameManager()
    return df_manager.create_dataframes(spark, temp_spec)

def execute_rule(spark, rule, rule_spec_data, dataframes):
    from rule_executor import RuleExecutor
    
    # Create dummy RuleSpec
    class TempRuleSpec:
        def __init__(self, rule, data_config, dictionary_config, as_of_date):
            self.rule = type('', (object,), rule)  # Simple object
            self.data_config = data_config
            self.dictionary_config = dictionary_config
            self.as_of_date = as_of_date
    
    temp_spec = TempRuleSpec(
        rule,
        rule_spec_data['data_config'],
        rule_spec_data['dictionary_config'],
        rule_spec_data['as_of_date']
    )
    executor = RuleExecutor()
    return executor.execute_rule(spark, temp_spec, dataframes)

def prepare_result_df(result_df, run_id, rule_id):
    result_df = result_df.withColumn("RULE_RUN_ID", lit(run_id))
    result_df = result_df.withColumn("RULE_ID", lit(rule_id))
    return result_df.select("RULE_RUN_ID", "RULE_ID", "HEADER_DATA", "COL1", "COL2", "COL3")

def write_results(result_df, run_id, rule_id):
    from result_exporter import ResultExporter
    hdfs_output_path = os.getenv("HDFS_OUTPUT_PATH", "/hdfs/rules_output")
    exporter = ResultExporter()
    exporter.export_results(result_df, run_id, rule_id, hdfs_output_path)

def update_execution_status(rule, run_id, status):
    from database_service import ExecutionStatusService
    from models import RuleExecutionStatus
    
    status_obj = RuleExecutionStatus(
        rule_run_id=run_id,
        rule_id=rule['rule_id'],
        rule_name=rule['rule_name'],
        rule_type=rule['rule_type'],
        rule_status=rule['rule_status'],
        module=rule['module'],
        stage=rule['stage'],
        package=rule['package'],
        slg=rule['slg'],
        program_type=rule['program_type'],
        program_code=rule['program_code'],
        rule_execution_status=status,
        execution_date=datetime.now().strftime("%Y-%m-%d"),
        processing_date=datetime.now().strftime("%Y-%m-%d"),
        exception_applied="No",
        action="",
        workflow_status="Completed" if status == "Success" else "Failed"
    )
    
    try:
        status_service = ExecutionStatusService()
        status_service.insert_execution_status(status_obj)
    except Exception as e:
        logger.error(f"Status update failed: {e}")

def main():
    args = parse_arguments()
    spark = None
    try:
        spark = create_spark_session(args.run_id)
        rule_spec_data = parse_rule_spec(args.rule_spec)
        rule = rule_spec_data['rule']
        
        # Process rule
        dataframes = create_dataframes(spark, rule_spec_data)
        result_df = execute_rule(spark, rule, rule_spec_data, dataframes)
        result_df = prepare_result_df(result_df, args.run_id, rule['rule_id'])
        write_results(result_df, args.run_id, rule['rule_id'])
        
        # Update status
        update_execution_status(rule, args.run_id, "Success")
        logger.info(f"Rule {rule['rule_id']} executed successfully")
        
    except Exception as e:
        logger.error(f"Rule execution failed: {e}")
        if 'rule' in locals():
            update_execution_status(rule, args.run_id, "Failure")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()


######################################## orchestrator.py ########################################
import logging
from typing import List, Optional
from models import Rule
from interfaces import IRuleService, IValidationEngine
from rule_configuration import RuleConfiguration
from rule_processor import RuleProcessor
from validation_engine import ValidationEngine
from database_service import RuleService, ExecutionStatusService, FeedMappingService
from spark_job_manager import SparkJobManager

logger = logging.getLogger(__name__)

class Orchestrator:
    def __init__(self, model: str, stage: str, as_of_date: str, packages: List[str], feed_id: Optional[str] = None):
        self.model = model
        self.stage = stage
        self.as_of_date = as_of_date
        self.packages = packages
        self.feed_id = feed_id
        
        # Services
        self.rule_service: IRuleService = RuleService()
        self.status_service = ExecutionStatusService()
        self.feed_service = FeedMappingService()
        
        # Components
        self.rule_config = RuleConfiguration()
        self.rule_processor = RuleProcessor(
            status_service=self.status_service,
            spark_job_manager=SparkJobManager()
        )
        self.validation_engine: IValidationEngine = ValidationEngine(
            status_service=self.status_service,
            feed_service=self.feed_service
        )

    def execute(self) -> bool:
        logger.info(f"Starting orchestration for {self.model}/{self.stage}")
        try:
            rules = self._load_rules()
            if not rules:
                logger.info("No rules to process")
                return True
            
            rule_specs = [self.rule_config.create_rule_spec(rule, self.as_of_date) for rule in rules]
            
            if not self.rule_processor.process_rules_parallel(rule_specs):
                return False
            
            return self.validation_engine.validate_rules([r.rule_id for r in rules])
        except Exception as e:
            logger.exception("Orchestration failed")
            return False

    def _load_rules(self) -> List[Rule]:
        all_rules = []
        for package in self.packages:
            rules = self.rule_service.get_active_rules_with_feeds(
                self.model, self.stage, package, self.feed_id
            )
            all_rules.extend(rules)
        logger.info(f"Loaded {len(all_rules)} rules")
        return all_rules


######################################## main.py ############################################
import argparse
import sys
from orchestrator import Orchestrator
from logger import setup_logging

def main():
    logger = setup_logging()
    args = parse_arguments()
    try:
        orchestrator = Orchestrator(
            model=args.model,
            stage=args.stage,
            as_of_date=args.as_of_date,
            packages=args.packages,
            feed_id=args.feed_id
        )
        success = orchestrator.execute()
        exit_code = 0 if success else 100
        logger.info(f"Process completed with exit code: {exit_code}")
        sys.exit(exit_code)
    except Exception as e:
        logger.exception("Critical failure in orchestration")
        sys.exit(1)

def parse_arguments():
    parser = argparse.ArgumentParser(description='Rules Engine Orchestrator')
    parser.add_argument('--model', required=True, help='Model name')
    parser.add_argument('--stage', required=True, help='Processing stage')
    parser.add_argument('--as_of_date', required=True, help='As of date (YYYYMMDD)')
    parser.add_argument('--packages', nargs='+', required=True, help='Packages to process')
    parser.add_argument('--feed_id', help='Optional feed ID for rule filtering')
    return parser.parse_args()

if __name__ == "__main__":
    main()