package mysql_provider

import (
	"database/sql"
	"github.com/go-sql-driver/mysql"
	"github.com/hashicorp/go-version"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"fmt"
	"log"
	"strings"
)


const defCharSetKey = "CHARACTER SET "
const defaultCollateKey = "COLLATE "
const unknownDatabaseErr = 1049

func ResourceDB() *schema.Resource {
	return &schema.Resource{
		Schema:             map[string]*schema.Schema{
			"name": {
				Type: schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"default_charset" : {
				Type: schema.TypeString,
				Optional: true,
				Default: "utf8",
			},
			"default_collation": {
				Type: schema.TypeString,
				Optional: true,
				Default: "utf8_general_ci",
			},
		},
		SchemaVersion:      0,
		MigrateState:       nil,
		StateUpgraders:     nil,
		Create:             CreateDb,
		Read:               nil,
		Update:             UpdateDb,
		Delete:             nil,
		Exists:             nil,
		CustomizeDiff:      nil,
		Importer:           nil,
		DeprecationMessage: "",
		Timeouts:           nil,
		Description:        "",
	}
}

func CreateDb(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*MySQLConfiguration).Db
	sqlStatment := databaseSQLCMD("CREATE", d)
	log.Println("Executing statement:", sqlStatment)
	_, err := db.Exec(sqlStatment)
	if err != nil {
		return err
	}
	d.SetId(d.Get("name").(string))

	return ReadDb(d, meta)
}

func ReadDb(d *schema.ResourceData, meta interface{}) error {
	db := meta.(*MySQLConfiguration).Db

	// This is kinda flimsy-feeling, since it depends on the formatting
	// of the SHOW CREATE DATABASE output... but this data doesn't seem
	// to be available any other way, so hopefully MySQL keeps this
	// compatible in future releases.

	name := d.Id()
	stmtSQL := "SHOW CREATE DATABASE " + quoteIdentifier(name)

	log.Println("Executing query:", stmtSQL)
	var createSQL, _database string
	err := db.QueryRow(stmtSQL).Scan(&_database, &createSQL)
	if err != nil {
		if mysqlErr, ok := err.(*mysql.MySQLError); ok {
			if mysqlErr.Number == unknownDatabaseErr {
				d.SetId("")
				return nil
			}
		}
		return fmt.Errorf("Error during show create database: %s", err)
	}

	defaultCharset := extractIdentAfter(createSQL, defCharSetKey)
	defaultCollation := extractIdentAfter(createSQL, defaultCollateKey)

	if defaultCollation == "" && defaultCharset != "" {
		// MySQL doesn't return the collation if it's the default one for
		// the charset, so if we don't have a collation we need to go
		// hunt for the default.
		stmtSQL := "SHOW COLLATION WHERE `Charset` = ? AND `Default` = 'Yes'"
		var empty interface{}

		requiredVersion, _ := version.NewVersion("8.0.0")
		currentVersion, err := mySQLServerVersion(db)
		if err != nil {
			return err
		}

		serverVersionString, err := mySQLServerVersionString(db)
		if err != nil {
			return err
		}

		// MySQL 8 returns more data in a row.
		var res error
		if !strings.Contains(serverVersionString, "MariaDB") && currentVersion.GreaterThan(requiredVersion) {
			res = db.QueryRow(stmtSQL, defaultCharset).Scan(&defaultCollation, &empty, &empty, &empty, &empty, &empty, &empty)
		} else {
			res = db.QueryRow(stmtSQL, defaultCharset).Scan(&defaultCollation, &empty, &empty, &empty, &empty, &empty)
		}

		if res != nil {
			if res == sql.ErrNoRows {
				return fmt.Errorf("Charset %s has no default collation", defaultCharset)
			}

			return fmt.Errorf("Error getting default charset: %s, %s", res, defaultCharset)
		}
	}

	d.Set("name", name)
	d.Set("default_character_set", defaultCharset)
	d.Set("default_collation", defaultCollation)

	return nil
}

func databaseSQLCMD(verb string, d *schema.ResourceData) string {
	name := d.Get("name").(string)
	defaultCharset := d.Get("default_character_set").(string)
	defaultCollation := d.Get("default_collation").(string)

	var defaultCharsetClause string
	var defaultCollationClause string

	if defaultCharset != "" {
		defaultCharsetClause = defCharSetKey + quoteIdentifier(defaultCharset)
	}
	if defaultCollation != "" {
		defaultCollationClause = defaultCollateKey + quoteIdentifier(defaultCollation)
	}

	return fmt.Sprintf(
		"%s DATABASE %s %s %s",
		verb,
		quoteIdentifier(name),
		defaultCharsetClause,
		defaultCollationClause,
	)
}

func extractIdentAfter(sql string, keyword string) string {
	charsetIndex := strings.Index(sql, keyword)
	if charsetIndex != -1 {
		charsetIndex += len(keyword)
		remain := sql[charsetIndex:]
		spaceIndex := strings.IndexRune(remain, ' ')
		return remain[:spaceIndex]
	}

	return ""
}