package qianji

import (
	"strings"

	"github.com/mayswind/ezbookkeeping/pkg/converters/datatable"
	"github.com/mayswind/ezbookkeeping/pkg/models"
	"github.com/mayswind/ezbookkeeping/pkg/utils"
)

var qianjiTransactionTypeNameMapping = map[models.TransactionType]string{
	models.TRANSACTION_TYPE_INCOME:   "收入",
	models.TRANSACTION_TYPE_EXPENSE:  "支出",
	models.TRANSACTION_TYPE_TRANSFER: "转账",
}

// qianjiTransactionDataRowParser defines the structure of qianji transaction data row parser
type qianjiTransactionDataRowParser struct {
}

// GetAddedColumns returns the added columns after converting the data row
func (p *qianjiTransactionDataRowParser) GetAddedColumns() []datatable.TransactionDataTableColumn {
	return nil
}

// Parse returns the converted transaction data row
func (p *qianjiTransactionDataRowParser) Parse(data map[datatable.TransactionDataTableColumn]string, rowId string) (rowData map[datatable.TransactionDataTableColumn]string, rowDataValid bool, err error) {
	rowData = make(map[datatable.TransactionDataTableColumn]string, len(data))

	for column, value := range data {
		rowData[column] = value
	}

	// 去除金额里可能存在的千位分隔符，防止 Excel 导出时带上逗号导致金额解析失败
	if rowData[datatable.TRANSACTION_DATA_TABLE_AMOUNT] != "" {
		rowData[datatable.TRANSACTION_DATA_TABLE_AMOUNT] = strings.ReplaceAll(rowData[datatable.TRANSACTION_DATA_TABLE_AMOUNT], ",", "")
	}

	if rowData[datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TIME] != "" {
		rowData[datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TIME] = p.getLongDateTime(rowData[datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TIME])
	}

	return rowData, true, nil
}

// getLongDateTime returns the normalized long date time string
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

// createQianjiTransactionDataRowParser returns qianji transaction data row parser
func createQianjiTransactionDataRowParser() datatable.TransactionDataRowParser {
	return &qianjiTransactionDataRowParser{}
}
