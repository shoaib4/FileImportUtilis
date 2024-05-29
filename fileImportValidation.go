package v2

import (
	"errors"
	"strings"
)

const (
	AverageByteToFetchHeader = 1000
)

const (
	ErrorFetchingHeader      = "error in fetching Header data for validation"
	ErrorFetchingContentType = "error in fetching content type for validation"
	ErrorTypeNotCSV          = "file type is not csv"
	ErrorHeaderDoesNotMatch  = "CSV header does not match"
)

type validationHelper struct {
	fia           *FileImportAuditUtility
	bucket        string
	inputUrl      string
	contentType   string
	contentLength int64
	headerData    []string
}

func NewValidationHelper(fia *FileImportAuditUtility, bucket string, inputUrl string) *validationHelper {
	return &validationHelper{
		fia:      fia,
		bucket:   bucket,
		inputUrl: inputUrl,
	}
}

func (v *validationHelper) getType() (string, error) {
	if v.contentType != "" {
		return v.contentType, nil
	}
	s3FileUrl := v.inputUrl
	folderPath := strings.TrimPrefix(s3FileUrl, s3Protocol+v.bucket+s3RegionArn)

	contentLength, contentType, err := v.fia.S3.GetFileLengthAndContentType(v.bucket, folderPath, v.fia.Logger.GetLogrusEntry())
	if err != nil {
		return "", errors.New(ErrorFetchingContentType)
	}
	v.contentType = contentType
	v.contentLength = contentLength
	return contentType, nil
}

func (v *validationHelper) getCSVHeader() ([]string, error) {
	if v.headerData != nil {
		return v.headerData, nil
	}
	response := v.fia.ProcessCSVDataWithFunction(
		v.bucket, v.inputUrl, AverageByteToFetchHeader,
		func(input CsvProcessingWithFunctionInput) (interface{}, error) {
			return input.Data, nil
		},
		SetProcessOnce(true),
		SetBreakOnError(true),
	)

	if len(response) > 0 {
		if response[0].Err != nil {
			return nil, errors.New(ErrorFetchingHeader)
		}
		data := response[0].Data.([][]string)
		if len(data) > 0 {
			row := data[0]
			v.headerData = row
			return row, nil
		}
	}
	return nil, errors.New(ErrorFetchingHeader)
}

func matchSlices(str1 []string, str2 []string) bool {
	if len(str1) != len(str2) {
		return false
	}
	for i, _ := range str1 {
		if str1[i] != str2[i] {
			return false
		}
	}
	return true
}

func IsType(str string) func(*validationHelper) error {
	return func(vv *validationHelper) error {
		contentType, err := vv.getType()
		if err != nil {
			return errors.New(ErrorFetchingContentType)
		}
		if contentType != str {
			return errors.New("type is not matching")
		}
		return nil
	}
}

func HasCSVHeader(h []string) func(*validationHelper) error {
	return func(vv *validationHelper) error {
		contentType, err := vv.getType()
		if err != nil {
			return errors.New(ErrorFetchingContentType)
		}
		if contentType != CsvContentType {
			return errors.New(ErrorTypeNotCSV)
		}
		header, err := vv.getCSVHeader()
		if err != nil {
			return errors.New(ErrorFetchingHeader)
		}
		if !matchSlices(h, header) {
			return errors.New(ErrorHeaderDoesNotMatch)
		}
		return nil
	}
}
