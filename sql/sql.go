package sql

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/olekukonko/tablewriter"
)

//ErrNotDbOrTx type error
var ErrNotDbOrTx = errors.New("db必须为sql.DB或sql.Tx类型")

//WaitingDb 等待db连接，直到成功，一般用在docker中，处理mysql和应用程序的启动顺序问题
func WaitingDb(db *sql.DB) {
	crt := time.Now()

	for {
		err := db.Ping()
		if err != nil {
			if time.Now().Sub(crt) > time.Second*30 {
				panic(err)
			}
			time.Sleep(time.Second)
		} else {
			break
		}
	}
}

//PrintfForQuery 打印查询表格
func PrintfForQuery(db interface{}, queryStr string) error {
	fmt.Println("执行sql:" + queryStr)
	var rows *sql.Rows
	var err error
	switch db.(type) {
	case *sql.DB:
		rows, err = db.(*sql.DB).Query(queryStr)
		if err != nil {
			return err
		}
		defer rows.Close()
	case *sql.Tx:
		rows, err = db.(*sql.Tx).Query(queryStr)
		if err != nil {
			return err
		}
		defer rows.Close()
	default:
		return ErrNotDbOrTx
	}
	columns, err := rows.Columns()
	if err != nil {
		panic(err)
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(columns)
	data := make([][]string, 0)

	for rows.Next() {
		row := make([]interface{}, len(columns))
		container := make([]string, len(columns))
		for i := range row {
			row[i] = &container[i]
		}
		rows.Scan(row...)
		data = append(data, container)
	}
	table.AppendBulk(data)
	table.Render()
	return nil
}

//PrintfForExec 打印执行sql
func PrintfForExec(db interface{}, queryStr string) error {
	fmt.Print("执行sql:" + queryStr)
	var result sql.Result
	var err error
	switch db.(type) {
	case *sql.DB:
		result, err = db.(*sql.DB).Exec(queryStr)
		if err != nil {
			return err
		}
	case *sql.Tx:
		result, err = db.(*sql.Tx).Exec(queryStr)
		if err != nil {
			return err
		}
	default:
		return ErrNotDbOrTx
	}
	rows, err := result.RowsAffected()
	if err != nil {
		switch db.(type) {
		case sql.Tx:
			db.(*sql.Tx).Rollback()
		}
		panic(err)
	}
	if rows > 0 {
		fmt.Printf(" 执行成功,影响了%d行\n", rows)
	} else {
		fmt.Println(" 执行失败,没有更改任何数据")
	}
	if err != nil {
		return err
	}
	return nil
}
