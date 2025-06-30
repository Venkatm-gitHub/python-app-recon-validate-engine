###################################### logger.py ######################################
import logging

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("rules_engine.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


###################################### models.py ######################################
from dataclasses import dataclass
from typing import Dict, List, Any, Optional
import json

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
    """Rule specification class for configuration management"""
    rule: Rule
    data_config: Dict[str, Any]
    dictionary_config: Dict[str, Any]
    as_of_date: str
    
    def get_table_config(self, table_name: str) -> Optional[TableConfig]:
        """Get table configuration by logical name"""
        for table in self.data_config.get('tables', []):
            if table['logicalName'] == table_name:
                return TableConfig(
                    logical_name=table['logicalName'],
                    path=table['path'],
                    kind=table['kind'],
                    schema=table.get('schema', {})
                )
        return None
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, str]]:
        """Get schema for a specific table"""
        for table in self.dictionary_config.get('tables', []):
            if table['logicalName'] == table_name:
                return table.get('schema', {}).get('RAW', [])
        return []


##################################### database_service.py #################################
from service import DatabaseService
from exceptions import DatabaseException
from models import Rule, RuleExecutionStatus
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class DatabaseServiceWrapper:
    def __init__(self):
        self.service = DatabaseService()

    def execute_query(self, query: str, params: dict) -> list:
        """Execute a query and return results"""
        with self.service.create_session('oracle') as session:
            results, _ = session.reader.read_as_tuple(query, params)
            return results

    def get_active_rules_with_feeds(self, model: str, stage: str, package: str, feed_id: str = None) -> List[Rule]:
        """Get active/passive rules filtered by feed configuration"""
        base_query = """
        SELECT DISTINCT r.RULE_ID, r.RULE_NAME, r.RULE_TYPE, r.RULE_STATUS, 
               r.MODULE, r.STAGE, r.PACKAGE, r.SLG, r.PROGRAM_TYPE, r.PROGRAM_CODE,
               r.CREATED_DT, r.CREATED_BY, r.MODIFIED_DT, r.MODIFIED_BY
        FROM TB_RULES_ENGINE_MST r
        """
        
        if feed_id:
            base_query += """
            INNER JOIN TB_RULES_FEED_CONFIG_MAP f ON r.RULE_ID = f.RULE_ID
            WHERE f.FEED_ID = :feed_id 
            AND f.RULE_APPLICABLE_STATUS = 'Yes'
            AND r.MODULE = :model
            AND r.STAGE = :stage
            AND r.PACKAGE = :package
            AND r.RULE_STATUS IN ('Active', 'Passive')
            """
            params = {'feed_id': feed_id, 'model': model, 'stage': stage, 'package': package}
        else:
            base_query += """
            WHERE r.MODULE = :model
            AND r.STAGE = :stage
            AND r.PACKAGE = :package
            AND r.RULE_STATUS IN ('Active', 'Passive')
            """
            params = {'model': model, 'stage': stage, 'package': package}

        try:
            results = self.execute_query(base_query, params)
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
        except Exception as e:
            logger.error(f"Failed to get rules: {e}")
            raise

    def get_runtime_approved_rules(self, rule_ids: List[str]) -> List[str]:
        """Get applicable rules from TB_RULES_RUNTIME_APPROVALS"""
        if not rule_ids:
            return []
            
        placeholders = ', '.join([f':id_{i}' for i in range(len(rule_ids))])
        query = f"""
        SELECT DISTINCT RULE_ID 
        FROM TB_RULES_RUNTIME_APPROVALS 
        WHERE RULE_ID IN ({placeholders})
        AND APPROVAL_STATUS = 'Approved'
        """
        
        params = {f'id_{i}': rule_id for i, rule_id in enumerate(rule_ids)}
        
        try:
            results = self.execute_query(query, params)
            return [row[0] for row in results]
        except Exception as e:
            logger.error(f"Failed to get runtime approved rules: {e}")
            return rule_ids  # Fallback to all rules if table doesn't exist

    def insert_execution_result(self, rule_run_id: str, rule_id: str, result_data: Dict[str, Any]):
        """Insert execution results into TB_RULES_EXECUTION_RESULT"""
        query = """
        INSERT INTO TB_RULES_EXECUTION_RESULT 
        (RULE_RUN_ID, RULE_ID, HEADER_DATA, COL1, COL2, COL3)
        VALUES (:1, :2, :3, :4, :5, :6)
        """
        
        params = (
            rule_run_id,
            rule_id,
            result_data.get('header_data', ''),
            result_data.get('col1', ''),
            result_data.get('col2', ''),
            result_data.get('col3', '')
        )
        
        try:
            with self.service.create_session('oracle') as session:
                session.writer.execute_write(query, params)
        except Exception as e:
            logger.error(f"Failed to insert execution result: {e}")
            raise

    def insert_execution_status(self, status: RuleExecutionStatus):
        """Insert execution status into TB_RULES_EXECUTION_STATUS"""
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
        """Get execution status for given rule IDs"""
        if not rule_ids:
            return []
            
        placeholders = ', '.join([f':id_{i}' for i in range(len(rule_ids))])
        query = f"""
        SELECT RULE_ID, RULE_EXECUTION_STATUS, EXCEPTION_APPLIED
        FROM TB_RULES_EXECUTION_STATUS
        WHERE RULE_ID IN ({placeholders})
        ORDER BY CREATED_DT DESC
        """
        
        params = {f'id_{i}': rule_id for i, rule_id in enumerate(rule_ids)}
        
        try:
            return self.execute_query(query, params)
        except Exception as e:
            logger.error(f"Failed to get execution status: {e}")
            raise


########################################### spark_manager.py #########################################
import subprocess
import uuid
import logging
import json
import os
from typing import Dict, Any
from models import Rule, RuleSpec

logger = logging.getLogger(__name__)

class EnhancedSparkManager:
    def __init__(self, data_config: Dict[str, Any], dict_config: Dict[str, Any]):
        self.data_config = data_config
        self.dict_config = dict_config

    def create_dataframes_from_config(self, spark_session, rule_spec: RuleSpec) -> Dict[str, Any]:
        """Create PySpark DataFrames dynamically from configuration files"""
        dataframes = {}
        
        for table in self.data_config.get('tables', []):
            logical_name = table['logicalName']
            path = table['path']
            file_format = table['kind'].get('RAW', 'PARQUET').upper()
            
            # Get schema from dictionary config
            schema_info = rule_spec.get_table_schema(logical_name)
            
            try:
                if file_format == 'PARQUET':
                    df = spark_session.read.parquet(path)
                elif file_format == 'DELTA':
                    df = spark_session.read.format("delta").load(path)
                elif file_format == 'CSV':
                    df = spark_session.read.option("header", "true").csv(path)
                else:
                    df = spark_session.read.format(file_format.lower()).load(path)
                
                # Apply schema if available
                if schema_info:
                    df = self._apply_schema_casting(df, schema_info)
                
                dataframes[logical_name] = df
                logger.info(f"Created DataFrame for {logical_name} from {path}")
                
            except Exception as e:
                logger.error(f"Failed to create DataFrame for {logical_name}: {e}")
                raise
        
        return dataframes

    def _apply_schema_casting(self, df, schema_info: list):
        """Apply schema casting based on dictionary configuration"""
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

    def execute_rule_logic(self, spark_session, rule_spec: RuleSpec, dataframes: Dict[str, Any]) -> Any:
        """Execute rule logic based on PROGRAM_TYPE (SQL vs Python DSL)"""
        rule = rule_spec.rule
        
        if rule.program_type.upper() == 'SQL':
            return self._execute_sql_rule(spark_session, rule, dataframes, rule_spec)
        elif rule.program_type.upper() == 'PYTHON':
            return self._execute_python_dsl_rule(spark_session, rule_spec, dataframes)
        else:
            raise ValueError(f"Unsupported program type: {rule.program_type}")

    def _execute_sql_rule(self, spark_session, rule: Rule, dataframes: Dict[str, Any], rule_spec: RuleSpec):
        """Fixed SQL rule execution with proper rule_spec access"""
        # Create temp views for all dataframes
        for logical_name, df in dataframes.items():
            df.createOrReplaceTempView(logical_name)
            logger.info(f"Created temp view: {logical_name}")
        
        # Read SQL from file
        try:
            with open(rule.program_code, 'r') as f:
                sql_query = f.read()
            
            # Replace date placeholders - FIX: Use rule_spec parameter
            sql_query = sql_query.replace('${as_of_date}', rule_spec.as_of_date)
            
            # Execute SQL
            result_df = spark_session.sql(sql_query)
            logger.info(f"Executed SQL rule: {rule.rule_id}")
            return result_df
            
        except Exception as e:
            logger.error(f"Failed to execute SQL rule {rule.rule_id}: {e}")
            raise

    def _execute_python_dsl_rule(self, spark_session, rule_spec: RuleSpec, dataframes: Dict[str, Any]):
        """Execute Python DSL rule"""
        rule = rule_spec.rule
        try:
            # Create execution context
            exec_context = {
                'spark': spark_session,
                'dataframes': dataframes,
                'as_of_date': rule_spec.as_of_date,
                'rule_id': rule.rule_id
            }

            # Execute Python code
            with open(rule.program_code, 'r') as f:
                python_code = f.read()

            exec(python_code, exec_context)

            if 'result_df' in exec_context:
                logger.info(f"Executed Python DSL rule: {rule.rule_id}")
                return exec_context['result_df']
            else:
                raise ValueError("Python rule must create 'result_df' variable")

        except Exception as e:
            logger.error(f"Failed to execute Python DSL rule {rule.rule_id}: {e}")
            raise


    def write_to_hdfs_and_sqoop(self, result_df, rule_run_id: str, rule_id: str, hdfs_output_path: str):
        """Write results to HDFS and sqoop to Oracle"""
        try:
            # Write to HDFS
            output_path = f"{hdfs_output_path}/{rule_id}/{rule_run_id}"
            result_df.write.mode("overwrite").parquet(output_path)
            logger.info(f"Written results to HDFS: {output_path}")
            
            # Prepare sqoop command
            sqoop_cmd = [
                "sqoop", "export",
                "--connect", os.getenv("ORACLE_JDBC_URL"),
                "--username", os.getenv("ORACLE_USERNAME"),
                "--password", os.getenv("ORACLE_PASSWORD"),
                "--table", "TB_RULES_EXECUTION_RESULT",
                "--export-dir", output_path,
                "--input-fields-terminated-by", "\t"
            ]
            
            # Execute sqoop
            result = subprocess.run(sqoop_cmd, check=True, capture_output=True, text=True)
            logger.info(f"Sqoop export completed: {result.stdout}")
            
        except Exception as e:
            logger.error(f"Failed to write/sqoop results: {e}")
            raise

    def submit_spark_job(self, rule_spec: RuleSpec, run_id: str) -> str:
        """Submit Spark job with enhanced driver"""
        rule = rule_spec.rule
        
        cmd = [
            "spark-submit",
            "--master", "yarn",
            "--deploy-mode", "cluster",
            "--name", f"Rule_{rule.rule_id}_{run_id}",
            "spark_driver.py",  # New enhanced driver
            "--run_id", run_id,
            "--rule_spec", json.dumps({
                'rule': rule.__dict__,
                'data_config': rule_spec.data_config,
                'dictionary_config': rule_spec.dictionary_config,
                'as_of_date': rule_spec.as_of_date
            })
        ]
        
        try:
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            logger.info(f"Spark job submitted successfully: {run_id}")
            return run_id
        except subprocess.CalledProcessError as e:
            logger.error(f"Spark job submission failed: {e.stderr}")
            raise


######################################## spark_driver.py ########################################
"""
Enhanced Spark Driver for Rule Execution
This file should be deployed to the cluster and called by spark-submit
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from models import Rule, RuleSpec, RuleExecutionStatus
from database_service import DatabaseServiceWrapper

# Centralized logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def update_execution_status(rule: Rule, run_id: str, status: str, error_msg: str = None):
    """Update execution status in Oracle database"""
    try:
        db_service = DatabaseServiceWrapper()
        status_obj = RuleExecutionStatus(
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
            rule_execution_status=status,
            execution_date=datetime.now().strftime("%Y-%m-%d"),
            processing_date=datetime.now().strftime("%Y-%m-%d"),
            exception_applied="No",
            action="",
            workflow_status="Completed" if status == "Success" else "Failed"
        )
        db_service.insert_execution_status(status_obj)
    except Exception as e:
        logger.error(f"Failed to update execution status: {str(e)}")

def main(args):
    """Main execution logic for Spark driver"""
    spark = None
    try:
        # Initialize Spark
        spark = SparkSession.builder \
            .appName(f"RuleExecution_{args.run_id}") \
            .getOrCreate()
        
        # Parse rule specification
        rule_spec_data = json.loads(args.rule_spec)
        rule = Rule(**rule_spec_data['rule'])
        rule_spec = RuleSpec(
            rule=rule,
            data_config=rule_spec_data['data_config'],
            dictionary_config=rule_spec_data['dictionary_config'],
            as_of_date=rule_spec_data['as_of_date']
        )
        
        # Initialize enhanced spark manager
        from spark_manager import EnhancedSparkManager
        spark_manager = EnhancedSparkManager(
            rule_spec.data_config, 
            rule_spec.dictionary_config
        )
        
        # Create DataFrames from config
        dataframes = spark_manager.create_dataframes_from_config(spark, rule_spec)
        
        # Execute rule logic
        result_df = spark_manager.execute_rule_logic(spark, rule_spec, dataframes)
        
        # Add required metadata columns
        result_df = result_df.withColumn("RULE_RUN_ID", lit(args.run_id))
        result_df = result_df.withColumn("RULE_ID", lit(rule.rule_id))
        
        # Select only required columns in order
        result_df = result_df.select(
            "RULE_RUN_ID",
            "RULE_ID",
            "HEADER_DATA",
            "COL1",
            "COL2",
            "COL3"
        )
        
        # Write to HDFS and Sqoop to Oracle
        hdfs_output_path = os.getenv("HDFS_OUTPUT_PATH", "/hdfs/rules_output")
        spark_manager.write_to_hdfs_and_sqoop(
            result_df, args.run_id, rule.rule_id, hdfs_output_path
        )
        
        # Update status to Success
        update_execution_status(rule, args.run_id, "Success")
        logger.info(f"Rule execution completed successfully: {rule.rule_id}")
        
    except Exception as e:
        logger.error(f"Rule execution failed: {str(e)}")
        # Update status to Failure
        update_execution_status(rule, args.run_id, "Failure", str(e))
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Enhanced Spark Rule Driver')
    parser.add_argument('--run_id', required=True)
    parser.add_argument('--rule_spec', required=True, help='JSON rule specification')
    
    args = parser.parse_args()
    main(args)


######################################## validation_engine.py ####################################
import logging
import time
from datetime import datetime, timedelta
from typing import List
from database_service import DatabaseServiceWrapper

logger = logging.getLogger(__name__)

class EnhancedValidationEngine:
    def __init__(self, db_service: DatabaseServiceWrapper):
        self.db_service = db_service

    def validate_rules(self, rule_ids: List[str]) -> bool:
        """Enhanced validation with TB_RULES_RUNTIME_APPROVALS integration"""
        logger.info("Starting enhanced rules validation")
        
        # Step 1: Get runtime approved rules
        approved_rule_ids = self.db_service.get_runtime_approved_rules(rule_ids)
        logger.info(f"Runtime approved rules: {len(approved_rule_ids)}")
        
        if not approved_rule_ids:
            logger.info("No runtime approved rules found")
            return True
        
        # Step 2: Check if all approved rules succeeded
        if self._all_rules_succeeded(approved_rule_ids):
            logger.info("All runtime approved rules succeeded")
            return True
        
        # Step 3: Start polling for exceptions
        return self._poll_for_exceptions(approved_rule_ids)

    def _all_rules_succeeded(self, rule_ids: List[str]) -> bool:
        """Check if all rules have succeeded"""
        statuses = self.db_service.get_execution_status(rule_ids)
        success_count = sum(1 for status in statuses if status[1] == "Success")
        logger.info(f"Success count: {success_count}/{len(rule_ids)}")
        return success_count == len(rule_ids)

    def _poll_for_exceptions(self, rule_ids: List[str]) -> bool:
        """Poll for exception status with 1-hour timeout"""
        logger.info("Starting exception polling")
        start_time = datetime.now()
        timeout = 3600  # 1 hour in seconds
        poll_interval = 60  # 1 minute
        
        while (datetime.now() - start_time).seconds < timeout:
            if self._exceptions_applied(rule_ids):
                logger.info("Exceptions applied for all failed rules")
                return True
            
            remaining_time = timeout - (datetime.now() - start_time).seconds
            logger.info(f"Waiting for exceptions... Remaining time: {remaining_time}s")
            time.sleep(poll_interval)
        
        # Timeout reached
        logger.error("Timeout waiting for exceptions to be applied")
        return False

    def _exceptions_applied(self, rule_ids: List[str]) -> bool:
        """Check if exceptions are applied for all failed rules"""
        statuses = self.db_service.get_execution_status(rule_ids)
        
        for status in statuses:
            rule_id, exec_status, exception_applied = status
            if exec_status != "Success" and exception_applied != "Yes":
                logger.debug(f"Rule {rule_id} still pending exception")
                return False
        
        return True


######################################## orchestrator.py ########################################
import logging
import json
from typing import List, Optional
from models import Rule, RuleSpec
from database_service import DatabaseServiceWrapper
from spark_manager import EnhancedSparkManager
from validation_engine import EnhancedValidationEngine
from rule_processor import EnhancedRuleProcessor

logger = logging.getLogger(__name__)

class EnhancedOrchestrator:
    def __init__(self, model: str, stage: str, as_of_date: str, packages: List[str], feed_id: Optional[str] = None):
        self.model = model
        self.stage = stage
        self.as_of_date = as_of_date
        self.packages = packages
        self.feed_id = feed_id
        
        # Load configurations
        self.data_config = self._load_config("data_config.json")
        self.dict_config = self._load_config("dictionary_config.json")
        
        # Initialize services
        self.db_service = DatabaseServiceWrapper()
        self.spark_manager = EnhancedSparkManager(self.data_config, self.dict_config)

    def _load_config(self, file_path: str) -> dict:
        """Load configuration files"""
        try:
            with open(file_path) as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"Config file not found: {file_path}")
            raise
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in config: {file_path}")
            raise

    def execute(self) -> bool:
        """Execute the complete orchestration workflow"""
        logger.info(f"Starting enhanced orchestration for model: {self.model}, stage: {self.stage}")
        
        try:
            # Step 1: Load rules with feed filtering
            rules = self._load_rules()
            if not rules:
                logger.info("No applicable rules found")
                return True
            
            # Step 2: Create rule specifications
            rule_specs = self._create_rule_specifications(rules)
            
            # Step 3: Process rules in parallel
            rule_processor = EnhancedRuleProcessor(self.db_service, self.spark_manager)
            processing_success = rule_processor.process_rules_parallel(rule_specs)
            
            if not processing_success:
                logger.error("Rule processing failed")
                return False
            
            # Step 4: Validate rules
            validation_engine = EnhancedValidationEngine(self.db_service)
            rule_ids = [rule.rule_id for rule in rules]
            validation_result = validation_engine.validate_rules(rule_ids)
            
            logger.info(f"Enhanced orchestration completed. Validation: {validation_result}")
            return validation_result
            
        except Exception as e:
            logger.exception("Enhanced orchestration failed")
            return False

    def _load_rules(self) -> List[Rule]:
        """Load rules with feed-based filtering"""
        all_rules = []
        
        for package in self.packages:
            rules = self.db_service.get_active_rules_with_feeds(
                self.model, self.stage, package, self.feed_id
            )
            all_rules.extend(rules)
        
        logger.info(f"Loaded {len(all_rules)} rules")
        return all_rules

    def _create_rule_specifications(self, rules: List[Rule]) -> List[RuleSpec]:
        """Create rule specifications for each rule"""
        rule_specs = []
        
        for rule in rules:
            rule_spec = RuleSpec(
                rule=rule,
                data_config=self.data_config,
                dictionary_config=self.dict_config,
                as_of_date=self.as_of_date
            )
            rule_specs.append(rule_spec)
        
        return rule_specs


######################################## rule_processor.py ######################################
import concurrent.futures
import logging
import time
import uuid
from datetime import datetime
from typing import List
from models import RuleSpec, RuleExecutionStatus
from database_service import DatabaseServiceWrapper
from spark_manager import EnhancedSparkManager

logger = logging.getLogger(__name__)

class EnhancedRuleProcessor:
    def __init__(self, db_service: DatabaseServiceWrapper, spark_manager: EnhancedSparkManager):
        self.db_service = db_service
        self.spark_manager = spark_manager
        self.execution_results = {}

    def process_rule(self, rule_spec: RuleSpec) -> bool:
        """Process a single rule with enhanced functionality"""
        rule = rule_spec.rule
        run_id = f"{uuid.uuid4().hex[:10]}_{int(time.time())}"
        logger.info(f"Processing enhanced rule: {rule.rule_id} with run_id: {run_id}")
        
        # Create audit entry - Start
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
        
        self.db_service.insert_execution_status(start_status)
        
        try:
            # Submit enhanced Spark job
            self.spark_manager.submit_spark_job(rule_spec, run_id)
            logger.info(f"Spark job submitted for rule: {rule.rule_id}")
            return True
            
        except Exception as e:
            logger.error(f"Enhanced rule processing failed: {rule.rule_id} - {str(e)}")
            
            # Update status with failure
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
            
            self.db_service.insert_execution_status(failure_status)
            return False

    def process_rules_parallel(self, rule_specs: List[RuleSpec]) -> bool:
        """Process multiple rules in parallel"""
        logger.info(f"Processing {len(rule_specs)} rules in parallel")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # Submit all rules for processing
            future_to_rule = {
                executor.submit(self.process_rule, rule_spec): rule_spec.rule
                for rule_spec in rule_specs
            }
            
            # Collect results
            success_count = 0
            for future in concurrent.futures.as_completed(future_to_rule):
                rule = future_to_rule[future]
                try:
                    result = future.result()
                    self.execution_results[rule.rule_id] = result
                    if result:
                        success_count += 1
                except Exception as e:
                    logger.error(f"Error processing rule {rule.rule_id}: {str(e)}")
                    self.execution_results[rule.rule_id] = False
        
        total_rules = len(rule_specs)
        logger.info(f"Parallel processing completed: {success_count}/{total_rules} successful")
        
        return success_count == total_rules  # Return True only if ALL rules succeeded


######################################## main.py ############################################
import argparse
import sys
import os
from orchestrator import EnhancedOrchestrator
from logger import setup_logging

def main():
    """Enhanced main function with comprehensive argument handling"""
    logger = setup_logging()
    
    parser = argparse.ArgumentParser(description='Enhanced Rules Engine Orchestrator')
    parser.add_argument('--model', required=True, help='Model name')
    parser.add_argument('--stage', required=True, help='Processing stage')
    parser.add_argument('--as_of_date', required=True, help='As of date (YYYYMMDD)')
    parser.add_argument('--packages', nargs='+', required=True, help='Packages to process')
    parser.add_argument('--feed_id', help='Optional feed ID for rule filtering')
    
    args = parser.parse_args()
    
    logger.info(f"Starting orchestration with parameters: {vars(args)}")
    
    try:
        # Initialize enhanced orchestrator
        orchestrator = EnhancedOrchestrator(
            model=args.model,
            stage=args.stage,
            as_of_date=args.as_of_date,
            packages=args.packages,
            feed_id=args.feed_id
        )
        
        # Execute orchestration
        success = orchestrator.execute()
        
        # Exit with appropriate code
        exit_code = 0 if success else 100
        logger.info(f"Orchestration completed with exit code: {exit_code}")
        sys.exit(exit_code)
        
    except Exception as e:
        logger.exception("Critical orchestration failure")
        sys.exit(1)

if __name__ == "__main__":
    main()