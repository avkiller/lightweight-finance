package qianji

import (
	"strings"
	"time"

	"github.com/mayswind/ezbookkeeping/pkg/converters/converter"
	"github.com/mayswind/ezbookkeeping/pkg/converters/datatable"
	"github.com/mayswind/ezbookkeeping/pkg/converters/excel"
	"github.com/mayswind/ezbookkeeping/pkg/core"
	"github.com/mayswind/ezbookkeeping/pkg/errs"
	"github.com/mayswind/ezbookkeeping/pkg/log"
	"github.com/mayswind/ezbookkeeping/pkg/models"
)

const qianjiAppTransactionTimeColumnName = "时间"
const qianjiAppTransactionTypeColumnName = "类型"
const qianjiAppTransactionCategoryColumnName = "分类"
const qianjiAppTransactionSubCategoryColumnName = "二级分类"
const qianjiAppTransactionAccountNameColumnName = "记账者"
const qianjiAppTransactionRelatedAccountNameColumnName = "账户2"
const qianjiAppTransactionAccountCurrencyColumnName = "币种"
const qianjiAppTransactionAmountColumnName = "金额"
const qianjiAppTransactionDescriptionColumnName = "备注"
const qianjiAppTransactionTagsColumnName = "标签"
const qianjiAppTransactionRelatedIdColumnName = "关联账单"

const qianjiAppTransactionTypeIncomeText = "收入"
const qianjiAppTransactionTypeExpenseText = "支出"

// 默认账户名称，当 Excel 中账户列为空时使用此名称填充
const qianjiAppTransactionDefaultAccountName = "fireworld"
const qianjiAppTransactionDefaultCurrency = "CNY"
const qianjiAppTransactionDefaultCategory = "其它杂项"
const qianjiAppTransactionDefaultSubCategory = "其它"

var qianjiAppDataColumnNameMapping = map[datatable.TransactionDataTableColumn]string{
	datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TIME:     qianjiAppTransactionTimeColumnName,
	datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TYPE:     qianjiAppTransactionTypeColumnName,
	datatable.TRANSACTION_DATA_TABLE_CATEGORY:             qianjiAppTransactionCategoryColumnName,
	datatable.TRANSACTION_DATA_TABLE_SUB_CATEGORY:         qianjiAppTransactionSubCategoryColumnName,
	datatable.TRANSACTION_DATA_TABLE_ACCOUNT_NAME:         qianjiAppTransactionAccountNameColumnName,
	datatable.TRANSACTION_DATA_TABLE_RELATED_ACCOUNT_NAME: qianjiAppTransactionRelatedAccountNameColumnName,
	datatable.TRANSACTION_DATA_TABLE_ACCOUNT_CURRENCY:     qianjiAppTransactionAccountCurrencyColumnName,
	datatable.TRANSACTION_DATA_TABLE_AMOUNT:               qianjiAppTransactionAmountColumnName,
	datatable.TRANSACTION_DATA_TABLE_DESCRIPTION:          qianjiAppTransactionDescriptionColumnName,
	datatable.TRANSACTION_DATA_TABLE_TAGS:                 qianjiAppTransactionTagsColumnName,
}

// qianjiAppTransactionDataXlsxFileImporter defines the structure of qianji app xlsx importer for transaction data
type qianjiAppTransactionDataXlsxFileImporter struct{}

// Initialize a qianji app transaction data xlsx file importer singleton instance
var (
	QianjiTransactionDataXlsxFileImporter = &qianjiAppTransactionDataXlsxFileImporter{}
)

// ParseImportedData returns the imported data by parsing the qianji app transaction xlsx data
func (c *qianjiAppTransactionDataXlsxFileImporter) ParseImportedData(ctx core.Context, user *models.User, data []byte, defaultTimezone *time.Location, additionalOptions converter.TransactionDataImporterOptions, accountMap map[string]*models.Account, expenseCategoryMap map[string]map[string]*models.TransactionCategory, incomeCategoryMap map[string]map[string]*models.TransactionCategory, transferCategoryMap map[string]map[string]*models.TransactionCategory, tagMap map[string]*models.TransactionTag) (models.ImportedTransactionSlice, []*models.Account, []*models.TransactionCategory, []*models.TransactionCategory, []*models.TransactionCategory, []*models.TransactionTag, error) {

	// 使用 excel 读取
	dataTable, err := excel.CreateNewExcelOOXMLFileBasicDataTable(data, true)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}

	commonDataTable := datatable.CreateNewCommonDataTableFromBasicDataTable(dataTable)

	if !commonDataTable.HasColumn(qianjiAppTransactionTimeColumnName) ||
		!commonDataTable.HasColumn(qianjiAppTransactionTypeColumnName) ||
		!commonDataTable.HasColumn(qianjiAppTransactionSubCategoryColumnName) ||
		!commonDataTable.HasColumn(qianjiAppTransactionAccountNameColumnName) ||
		!commonDataTable.HasColumn(qianjiAppTransactionAmountColumnName) {
		log.Errorf(ctx, "[qianji_app_transaction_data_xlsx_file_importer.ParseImportedData] cannot parse import data, because missing essential columns in header row")
		return nil, nil, nil, nil, nil, nil, errs.ErrMissingRequiredFieldInHeaderRow
	}

	transactionDataTable, err := c.createNewQianjiAppTransactionDataTable(ctx, commonDataTable)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}

	dataTableImporter := converter.CreateNewSimpleImporterWithTypeNameMapping(qianjiTransactionTypeNameMapping)

	return dataTableImporter.ParseImportedData(ctx, user, transactionDataTable, defaultTimezone, additionalOptions, accountMap, expenseCategoryMap, incomeCategoryMap, transferCategoryMap, tagMap)
}

func (c *qianjiAppTransactionDataXlsxFileImporter) createNewQianjiAppTransactionDataTable(ctx core.Context, commonDataTable datatable.CommonDataTable) (datatable.TransactionDataTable, error) {
	// 强制包含所有列（照你截图左侧的老代码风格）
	newColumns := make([]datatable.TransactionDataTableColumn, 0, 12)
	newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TYPE)
	newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TIME)
	newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_CATEGORY)
	newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_SUB_CATEGORY)
	newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_ACCOUNT_NAME)
	newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_ACCOUNT_CURRENCY)
	newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_AMOUNT)
	newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_RELATED_ACCOUNT_NAME)
	newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_RELATED_ACCOUNT_CURRENCY)
	newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_RELATED_AMOUNT)
	newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_DESCRIPTION)
	newColumns = append(newColumns, datatable.TRANSACTION_DATA_TABLE_TAGS)

	transactionRowParser := createQianjiTransactionDataRowParser()
	transactionDataTable := datatable.CreateNewWritableTransactionDataTableWithRowParser(newColumns, transactionRowParser)

	commonDataTableIterator := commonDataTable.DataRowIterator()

	for commonDataTableIterator.HasNext() {
		dataRow := commonDataTableIterator.Next()
		rowId := commonDataTableIterator.CurrentRowId()

		data := make(map[datatable.TransactionDataTableColumn]string, 12)

		for columnType, columnName := range qianjiAppDataColumnNameMapping {
			if dataRow.HasData(columnName) {
				val := dataRow.GetData(columnName)
				val = strings.ReplaceAll(val, "\u00A0", " ")
				val = strings.ReplaceAll(val, "\u3000", " ")
				// 重点：过滤掉转账专属字段，避免普通收支下出现未知错误
				if columnType == datatable.TRANSACTION_DATA_TABLE_RELATED_ACCOUNT_NAME ||
					columnType == datatable.TRANSACTION_DATA_TABLE_RELATED_ACCOUNT_CURRENCY ||
					columnType == datatable.TRANSACTION_DATA_TABLE_RELATED_AMOUNT {
					continue
				}
				data[columnType] = strings.TrimSpace(val)
			}
		}

		transactionType := data[datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TYPE]
		accountName := data[datatable.TRANSACTION_DATA_TABLE_ACCOUNT_NAME]
		relatedAccountName := data[datatable.TRANSACTION_DATA_TABLE_RELATED_ACCOUNT_NAME]
		amount := data[datatable.TRANSACTION_DATA_TABLE_AMOUNT]
		currency := data[datatable.TRANSACTION_DATA_TABLE_ACCOUNT_CURRENCY]
		category := data[datatable.TRANSACTION_DATA_TABLE_CATEGORY]
		subCategory := data[datatable.TRANSACTION_DATA_TABLE_SUB_CATEGORY]

		// 过滤空行
		if transactionType == "" && amount == "" && accountName == "" && relatedAccountName == "" {
			continue
		}

		// 1. 处理转账
		if accountName != "" && relatedAccountName != "" {
			data[datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TYPE] = qianjiTransactionTypeNameMapping[models.TRANSACTION_TYPE_TRANSFER]
			// 填充转账必须的字段
			data[datatable.TRANSACTION_DATA_TABLE_RELATED_ACCOUNT_NAME] = relatedAccountName
			data[datatable.TRANSACTION_DATA_TABLE_RELATED_AMOUNT] = amount
			if data[datatable.TRANSACTION_DATA_TABLE_RELATED_ACCOUNT_CURRENCY] == "" {
				data[datatable.TRANSACTION_DATA_TABLE_RELATED_ACCOUNT_CURRENCY] = currency
			}
			if currency == "" {
				data[datatable.TRANSACTION_DATA_TABLE_ACCOUNT_CURRENCY] = qianjiAppTransactionDefaultCurrency
				data[datatable.TRANSACTION_DATA_TABLE_RELATED_ACCOUNT_CURRENCY] = qianjiAppTransactionDefaultCurrency
			}
			transactionDataTable.Add(data)
			continue
		}
		// 2. 处理收入/支出
		if transactionType == qianjiAppTransactionTypeIncomeText {
			data[datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TYPE] = qianjiTransactionTypeNameMapping[models.TRANSACTION_TYPE_INCOME]

		} else if transactionType == qianjiAppTransactionTypeExpenseText {
			data[datatable.TRANSACTION_DATA_TABLE_TRANSACTION_TYPE] = qianjiTransactionTypeNameMapping[models.TRANSACTION_TYPE_EXPENSE]
		} else {
			log.Errorf(ctx, "[qianji] cannot parse transaction type \"%s\" in \"%s\"", transactionType, rowId)
			return nil, errs.NewImportError(errs.ErrTransactionTypeInvalid, rowId, transactionType)
		}

		// 账户为空，填充默认值
		if accountName == "" {
			data[datatable.TRANSACTION_DATA_TABLE_ACCOUNT_NAME] = qianjiAppTransactionDefaultAccountName
		}

		// 币种为空，填充默认值
		if currency == "" {
			data[datatable.TRANSACTION_DATA_TABLE_ACCOUNT_CURRENCY] = qianjiAppTransactionDefaultCurrency
		}

		// 🌟 核心兜底逻辑：只要有分类为空，就填充“其他”
		if subCategory == "" {
			// 如果一级分类为空，也强制填“其他”
			if category == "" {
				data[datatable.TRANSACTION_DATA_TABLE_CATEGORY] = qianjiAppTransactionDefaultCategory
			}
			// 二级分类为空，强制填“其他”
			data[datatable.TRANSACTION_DATA_TABLE_SUB_CATEGORY] = qianjiAppTransactionDefaultSubCategory
		} else if category == "" {
			// 如果只有二级分类有值，一级分类为空，默认给一级分类填“其他”
			data[datatable.TRANSACTION_DATA_TABLE_CATEGORY] = qianjiAppTransactionDefaultCategory
		}

		// 🌟 重点修改：普通收支，不要往 data 里传那 3 个转账专属字段
		delete(data, datatable.TRANSACTION_DATA_TABLE_RELATED_ACCOUNT_NAME)
		delete(data, datatable.TRANSACTION_DATA_TABLE_RELATED_ACCOUNT_CURRENCY)
		delete(data, datatable.TRANSACTION_DATA_TABLE_RELATED_AMOUNT)
		transactionDataTable.Add(data)
	}

	return transactionDataTable, nil
}
