package qianji

import (
	"github.com/mayswind/ezbookkeeping/pkg/converters/datatable"
	// "github.com/mayswind/ezbookkeeping/pkg/errs"
	"github.com/mayswind/ezbookkeeping/pkg/models"
	"github.com/mayswind/ezbookkeeping/pkg/utils"
)

var qianjiTransactionTypeNameMapping = map[models.TransactionType]string{
	models.TRANSACTION_TYPE_INCOME:  "收入",
	models.TRANSACTION_TYPE_EXPENSE: "支出",
}

// qianjiTransactionDataRowParser defines the structure of feidee mymoney transaction data row parser
type qianjiTransactionDataRowParser struct {
}

// GetAddedColumns returns the added columns after converting the data row
func (p *qianjiTransactionDataRowParser) GetAddedColumns() []datatable.TransactionDataTableColumn {
	return nil
}

// Parse returns the converted transaction data row
func (p *qianjiTransactionDataRowParser) Parse(data map[datatable.TransactionDataTableColumn]string) (rowData map[datatable.TransactionDataTableColumn]string, rowDataValid bool, err error) {
	rowData = make(map[datatable.TransactionDataTableColumn]string, len(data))

	for column, value := range data {
		rowData[column] = value
	}

	if rowData[datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TIME] != "" {
		rowData[datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TIME] = p.getLongDateTime(rowData[datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TIME])
	}

	return rowData, true, nil
}

// Parse returns the converted transaction data row
func (p *qianjiTransactionDataRowParser) getLongDateTime(str string) string {
	if utils.IsValidLongDateTimeFormat(str) {
		return str
	}

	if utils.IsValidLongDateTimeWithoutSecondFormat(str) {
		return str + ":00"
	}

	if utils.IsValidLongDateFormat(str) {
		return str + " 00:00:00"
	}

	return str
}

// createqianjiTransactionDataRowParser returns feidee mymoney transaction data row parser
func createQianjiTransactionDataRowParser() datatable.TransactionDataRowParser {
	return &qianjiTransactionDataRowParser{}
}
