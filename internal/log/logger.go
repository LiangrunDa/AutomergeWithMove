package log

import "github.com/sirupsen/logrus"

type TransactionFormatter struct {
	txnId        string
	analysisMode bool
}

func NewTransactionFormatter(txnId string, analysisMode bool) *TransactionFormatter {
	return &TransactionFormatter{txnId: txnId, analysisMode: analysisMode}
}

func (f *TransactionFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	entry.Data["txnId"] = f.txnId

	if !f.analysisMode {
		return (&logrus.TextFormatter{ForceColors: true, DisableColors: false, TimestampFormat: "Jan _2 15:04:05.000000", FullTimestamp: true}).Format(entry)
	} else {
		return (&logrus.JSONFormatter{TimestampFormat: "Jan _2 15:04:05.000000"}).Format(entry)
	}
}

type ApplyingFormatter struct {
	changeId     string
	analysisMode bool
}

func NewApplyingFormatter(changeId string, analysisMode bool) *ApplyingFormatter {
	return &ApplyingFormatter{changeId: changeId, analysisMode: analysisMode}
}
func (f *ApplyingFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	entry.Data["chId"] = f.changeId
	if !f.analysisMode {
		return (&logrus.TextFormatter{ForceColors: true, DisableColors: false, TimestampFormat: "Jan _2 15:04:05.000000", FullTimestamp: true}).Format(entry)
	} else {
		return (&logrus.JSONFormatter{TimestampFormat: "Jan _2 15:04:05.000000"}).Format(entry)
	}
}

func MinimalTracef(format string, computeFunc func() string) {
	if logrus.GetLevel() >= logrus.TraceLevel {
		logrus.Tracef(format, computeFunc())
	}
}
