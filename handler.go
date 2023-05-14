package main

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
)

type Handler interface {
	Handle(string) error
}

type MyHandler struct {
	db            *sql.DB
	outputDirPath string
}

type UnitData struct {
	n         string
	mqtt      string
	invid     string
	unitGuid  string
	msgId     string
	text      string
	context   string
	class     string
	level     string
	area      string
	addr      string
	block     string
	type_     string
	bit       string
	invertBit string
}

func (u *UnitData) New(rec []string) {
	u.n = rec[0]
	u.mqtt = rec[1]
	u.invid = rec[2]
	u.unitGuid = rec[3]
	u.msgId = rec[4]
	u.text = rec[5]
	u.context = rec[6]
	u.class = rec[7]
	u.level = rec[8]
	u.area = rec[9]
	u.addr = rec[10]
	u.block = rec[11]
	u.type_ = rec[12]
	u.bit = rec[13]
	u.invertBit = rec[14]
}

func (u *UnitData) Dump(out *os.File) {
	fmt.Fprintln(out, "mqtt", u.mqtt)
	fmt.Fprintln(out, "invid", u.invid)
	fmt.Fprintln(out, "msgId", u.msgId)
	fmt.Fprintln(out, "text", u.text)
	fmt.Fprintln(out, "context", u.context)
	fmt.Fprintln(out, "class", u.class)
	fmt.Fprintln(out, "level", u.level)
	fmt.Fprintln(out, "area", u.area)
	fmt.Fprintln(out, "addr", u.addr)
	fmt.Fprintln(out, "block", u.block)
	fmt.Fprintln(out, "type", u.type_)
	fmt.Fprintln(out, "bit", u.bit)
	fmt.Fprintln(out, "invertBit", u.invertBit)
}

func (h *MyHandler) Handle(file string) error {
	fileReader, err := os.Open(file)
	if err != nil {
		return err
	}
	defer func(fileReader *os.File) {
		err := fileReader.Close()
		if err != nil {
			log.Println("datafile closing error", err)
		}
	}(fileReader)

	tsvReader := csv.NewReader(fileReader)
	tsvReader.Comma = '\t'
	tsvReader.FieldsPerRecord = 15

	var unitData UnitData
	for {
		record, err := tsvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			errLog := h.logError(errors.New("error parsing file"))
			if errLog != nil {
				log.Println("logger error", errLog)
			}
			return err
		}

		unitData.New(record)
		_, err = h.db.Exec("INSERT INTO mytable (unitGuid, mqtt, invid, msgId, text, context, class, level,"+
			" area,"+
			" addr, block, type_, bit, invertBit) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
			unitData.unitGuid, unitData.mqtt, unitData.invid, unitData.msgId, unitData.text, unitData.context,
			unitData.class, unitData.level, unitData.area, unitData.addr, unitData.block, unitData.type_, unitData.bit,
			unitData.invertBit)
		if err != nil {
			errLog := h.logError(err)
			if errLog != nil {
				log.Println("logger error", errLog)
			}
			return err
		}
	}

	outputFilePath := h.outputDirPath + "/" + unitData.unitGuid + ".rtf"
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		errLog := h.logError(err)
		if errLog != nil {
			log.Println("logger error", errLog)
		}
		return err
	}
	unitData.Dump(outputFile)
	err = outputFile.Close()
	if err != nil {
		log.Println("output file closing error", err)
	}
	return nil
}

func (h *MyHandler) logError(err error) error {
	_, err = h.db.Exec("INSERT INTO error_log (error_message) VALUES ($1)", err.Error())
	if err != nil {
		return err
	}
	errorFilePath := h.outputDirPath + "/" + "error_log.txt"
	errorFile, _ := os.Open(errorFilePath)
	_, _ = errorFile.WriteString(err.Error() + "\n")
	if err != nil {
		return err
	}
	err = errorFile.Close()
	if err != nil {
		log.Println("error_log file closing error", err)
	}
	return nil
}

func (h *MyHandler) getData(page int, limit int) ([]UnitData, error) {
	var units []UnitData
	offset := (page - 1) * limit
	rows, err := h.db.Query("SELECT * FROM mytable ORDER BY id LIMIT $1 OFFSET $2", limit, offset)
	if err != nil {
		return units, err
	}

	for rows.Next() {
		var unit UnitData
		err = rows.Scan(&unit.unitGuid, &unit.mqtt, &unit.invid, &unit.msgId, &unit.text, &unit.context,
			&unit.class, &unit.level, &unit.area, &unit.addr, &unit.block, &unit.type_, &unit.bit, &unit.invertBit)
		if err != nil {
			return units, err
		}
		units = append(units, unit)
	}

	return units, nil
}
