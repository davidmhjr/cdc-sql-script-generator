<?php

namespace Exac\Cdc;

require_once EXAC_MULTISITE_DATA_SYNC_PLUGIN_DIR . '/cdc-config.php';

use \PDO;

/**
 * Class Cdc_Create_Sql_Generator
 * @package Exac\Cdc
 */
class Cdc_Create_Sql_Generator
{

	/**
	 * @var PDO
	 */
	private $db;

	/**
	 * CDC method types.
	 */
	private $events = array(
		'ins' => 'INSERT',
		'upd' => 'UPDATE',
		'del' => 'DELETE'
	);

	/**
	 * @var string
	 */
	private $cdc_prefix;

	/**
	 * @var string
	 */
	private $source_prefix;

	/**
	 * @var string Name of the file for SQL output.
	 */
	private $sql_filename = 'cdc_create_%s.sql';

	/**
	 * @var string
	 */
	private $path;

	/**
	 * @var array
	 */
	private $tables;


	/**
	 * Exac_Multisite_Cdc_Sql_File_Creator constructor.
	 */
	public function __construct()
	{
		$this->path = EXAC_MULTISITE_DATA_SYNC_PLUGIN_DIR . '/sql/generated/';

		if ( !file_exists( $this->path ) ) {
			mkdir( $this->path, 0755 );
		}

		$this->sql_filename  = sprintf( $this->sql_filename, '' );
		$this->cdc_prefix    = CDC_TABLE_PREFIX;
		$this->source_prefix = CONTENT_TABLE_PREFIX;
		$this->tables        = cdc_config()->get_cdc_base_table_names();

		$this->db = new PDO(
			"mysql:dbname=" . DB_NAME . ";host=" . DB_HOST, DB_USER, DB_PASSWORD
		);
	}


	/**
	 * @return string
	 */
	public function create_sql_file()
	{
		$file = $this->path . $this->sql_filename;

		file_put_contents( $file, "/* Creates CDC tables and data capture triggers */\n\n" );

		// create the CDC table for each original table in the db...
		foreach ( $this->tables as $table ) {
			$tbl_cdc = "{$this->cdc_prefix}{$table}";
			$tbl_src = "{$this->source_prefix}{$table}";
			$tbl_log = "{$this->cdc_prefix}inserts";

			// gather column information for the table...
			$table_description = $this->db->query( "DESCRIBE $tbl_src" )->fetchAll();

			// start assembling the create CDC table query...
			$query = "DROP TABLE IF EXISTS {$tbl_cdc};\n";
			$query .= "CREATE TABLE ";
			$query .= "$tbl_cdc (\n";

			// create each CDC table column from original table info...
			foreach ( $table_description as $column ) {
				if ( $column['Extra'] === 'auto_increment' ) {
					$column['Type'] = 'bigint(20) unsigned';
				}

				$collation = '';
				$can_collate =
					stripos( ' ' . $column['Type'], 'text' ) + stripos( ' ' . $column['Type'], 'varchar' );

				if ( $can_collate > 0 ) {
					$collation = ' COLLATE utf8mb4_unicode_520_ci';
				}

				$query .= sprintf(
					"  %s %s%s,\n",
					$column['Field'], $column['Type'], $collation
				);
			}

			$query .= "  change_type char(1),\n";
			$query .= "  capture_date datetime\n";
			$query .= ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n";
			$query .= "\n\n";

			file_put_contents( $file, $query, FILE_APPEND );

			foreach ( $this->events as $key => $event ) {
				$trigger_name = "tr_{$key}_{$tbl_cdc}";

				// default is for handling inserts and updates...
				$key == 'del' ? $direction = 'old' : $direction = 'new';
				$action_time = 'AFTER';

				$trigger  = "DROP TRIGGER IF EXISTS $trigger_name;\n";
				$trigger .= "DELIMITER $$\n";
				$trigger .= "CREATE TRIGGER $trigger_name\n  $action_time $event ON $tbl_src\n";
				$trigger .= "FOR EACH ROW\n";
				$trigger .= "BEGIN\n";
				$trigger .= "  INSERT INTO $tbl_cdc (change_type, capture_date";

				$col_list = '';

				// each original field...
				foreach ( $table_description as $column ) {
					$col_list .= ", {$column['Field']}";
				}

				$trigger .= $col_list;
				$trigger .= ")\n";
				$trigger .= "  VALUES ('{$event[0]}', now()"; // adds I, D or U for change_type...

				// insert into new CDC field...
				$trigger .= $col_list;
				$trigger .= ");\n";
				$trigger .= "\n";

				$origin = substr( $tbl_cdc, strlen( $this->cdc_prefix) );

				$trigger .= "  INSERT INTO $tbl_log (origin) \n";
				$trigger .= "  VALUES ('$origin');\n";
				$trigger .= "END";
				$trigger .= " $$\n";
				$trigger .= "DELIMITER ;\n";
				$trigger .= "\n\n";

				file_put_contents( $file, $trigger, FILE_APPEND );
			}
		}

		$log_sql =
			"CREATE TABLE IF NOT EXISTS cdc_inserts (
				origin VARCHAR(30) NOT NULL
			)  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_520_ci";

		file_put_contents( $file, $log_sql, FILE_APPEND );

		return $file;
	}
}