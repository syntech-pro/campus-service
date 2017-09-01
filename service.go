package service

import (
	"encoding/binary"
	"encoding/xml"
	"errors"
	"fmt"
	"net"
	"time"

	"io"

	"github.com/golang/glog"
)

const DATA_HEADER_LENGTH = 4
const READ_BUFFER_SIZE = 1024 * 64
const RESPONSE_BUFFER_SIZE = 1024 * 1024
const TCP_READ_TIMEOUT = 5
const CAMPUS_SERVICE_TIMEOUT = 5
const CAMPUS_SERVICE_QUEUE_SIZE = 10
const CAMPUS_DATE_FORMAT = "2006-01-02T15:04:05"

// TODO: pool of connections

type CampusService struct {
	conf    CampusConfig
	connect *net.TCPConn
	chReq   chan CampusExchange
}

type CampusConfig struct {
	Host                       string
	CardBin                    string
	AccountBin                 string
	SamReaderType              string
	SamReaderDevice            string
	SamUser                    string
	SamPassword                string
	ContactlessReaderType      string
	ContactlessReaderDevice    string
	AuthHostAddress            string
	AuthHostPort               string
	AuthHostURITemplate        string
	CardholdersHostAddress     string
	CardholdersHostPort        string
	CardholdersHostURITemplate string
	Organization               string
	ImportDirectory            string
	ExportDirectory            string
	TimeoutReinit              int64
	BalanceRelevance           int64
	StatementPeriod            int64
}

type Status struct {
	Code    string `xml:",attr"`
	Message string `xml:",attr"`
}

type ResponseStatus struct {
	XMLName xml.Name `xml:"Response"`
	Status  Status
}

type ResponseSamReaderStatus struct {
	XMLName         xml.Name `xml:"Response"`
	SamReaderStatus Status
	Status          Status
}

type ResponseContactlessReaderStatus struct {
	XMLName                 xml.Name `xml:"Response"`
	ContactlessReaderStatus Status
	Status                  Status
}

type CardBalance struct {
	XMLName xml.Name `xml:"CardBalance"`
	Balance string   `xml:",attr"`
	//	BalanceTime  time.Time `xml:",attr"`
	CampusNamber string `xml:",attr"`
}

type ResponseCardBalance struct {
	XMLName     xml.Name `xml:"Response"`
	CardBalance CardBalance
	Status      Status
}

type ResponseCardholderData struct {
	XMLName        xml.Name `xml:"Response"`
	CardholderData CardholderData
	Status         Status
}

type CardholderData struct {
	XMLName           xml.Name `xml:"CardholderData"`
	PersonalData      PersonalData
	EducationalData   EducationalData
	Document          Document
	AddressCollection AddressCollection `xml:"Address"`
	Photo             string            `xml:"Photo"`
}

type PersonalData struct {
	XMLName      xml.Name `xml:"PersonalData"`
	BirthDay     string   `xml:",attr"`
	CampusCard   string   `xml:",attr"`
	FamilyStatus string   `xml:",attr"`
	FirstName    string   `xml:",attr"`
	LastName     string   `xml:",attr"`
	MidName      string   `xml:",attr"`
	INN          string   `xml:",attr"`
	SNILS        string   `xml:",attr"`
	Sex          string   `xml:",attr"`
}

type EducationalData struct {
	XMLName         xml.Name `xml:"EducationalData"`
	DivisionID      string   `xml:",attr"`
	DogNomer        string   `xml:",attr"`
	GroupID         string   `xml:",attr"`
	Speciality      string   `xml:",attr"`
	StudbiletNumber string   `xml:",attr"`
}

type Document struct {
	XMLName   xml.Name `xml:"Document"`
	DocCode   string   `xml:",attr"`
	DocNumber string   `xml:",attr"`
	DocSeriya string   `xml:",attr"`
	DocWhen   string   `xml:",attr"`
	DocWho    string   `xml:",attr"`
}

type AddressCollection struct {
	//	XMLName   xml.Name `xml:"Address"`
	RegAddress  Address `xml:"RegAddress"`
	FactAddress Address `xml:"FactAddress"`
	PostAddress Address `xml:"PostAddress"`
}

type Address struct {
	//	XMLName xml.Name `xml:"RegAddress"`
	Country string `xml:"Country,attr"`
	Flat    string `xml:"Flat,attr"`
	House   string `xml:"House,attr"`
	Index   string `xml:"Index,attr"`
}

type AuthorisData struct {
	XMLName      xml.Name      `xml:"AuthorisData"`
	Transactions []Transaction `xml:"Data"`
}

type ResponseAuthorisData struct {
	XMLName      xml.Name `xml:"Response"`
	AuthorisData AuthorisData
	Status       Status
}

type Transaction struct {
	CampusNumber     string `xml:",attr"`
	OperCode         string `xml:",attr"`
	State            string `xml:",attr"`
	Summa            string `xml:",attr"`
	TaskID           string `xml:",attr"`
	TaskUniqueNumber string `xml:",attr"`
	Timestamp        string `xml:",attr"`
}

type CampusExchange struct {
	Data         []byte
	ChanResponse chan []byte
	ChanError    chan error
	InProcess    bool
}

func InitCampusService(config CampusConfig) (cs CampusService, err error) {

	cs = CampusService{conf: config}

	// Init reader goroutine and channels
	// TODO: read timeout
	cs.chReq = make(chan CampusExchange, CAMPUS_SERVICE_QUEUE_SIZE)

	go cs.exchange()

	// Start ticker for reinit campus service
	ticker := time.NewTicker(time.Second * time.Duration(cs.conf.TimeoutReinit))
	go func() {
		for t := range ticker.C {
			if cs.connect == nil {
				glog.Info("Connect to campus service is absent. Reinit connection at ", t)
				err := cs.initConnect()
				if err != nil {
					glog.Error("Campus service init connect failed:", err.Error())
				}
			} else {
				glog.Info("Reinit campus service at ", t)
				err := cs.AuthorizeUser()
				if err != nil {
					glog.Error("Reinit campus service failed:", err.Error())
					// TODO what doing?
				}
			}
		}
	}()

	// Init connection to campus service
	err = cs.initConnect()
	if err != nil {
		glog.Error("InitCampusService(): Campus service init connect failed:", err.Error())
		return
	}

	return
}

func (service *CampusService) Close() {
	service.connect.Close()
	service.connect = nil
}

// Init tcp connection and init campus service
func (service *CampusService) initConnect() error {
	if service.connect != nil {
		glog.Warning("initConnect(): Campus service connect is active but init recieve")
		service.Close()
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", service.conf.Host)
	if err != nil {
		glog.Error("initConnect(): ResolveTCPAddr failed:", err.Error())
		service.connect = nil
		return err
	}
	service.connect, err = net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		glog.Error("initConnect(): Dial failed:", err.Error())
		service.connect = nil
		return err
	}

	glog.Info("initConnect(): Init tcp connection to campus service complete")

	// Init campus service
	err = service.GetSamReaderStatus()
	if err != nil {
		glog.Error("initConnect(): GetSamReaderStatus fail: ", err.Error())
		service.connect.Close()
		service.connect = nil
		return err
	}
	err = service.SetSamReader()
	if err != nil {
		glog.Error("initConnect(): SetSamReader fail: ", err.Error())
		service.Close()
		return err
	}
	if service.conf.ContactlessReaderDevice != "" {
		err = service.SetContactlessReader()
		if err != nil {
			glog.Error("initConnect(): SetContactlessReader fail: ", err.Error())
			service.Close()
			return err
		}
	}
	err = service.SetAuthorizParams()
	if err != nil {
		glog.Error("SetAuthorizParams fail", err.Error())
		return err
	}
	err = service.SetImportDirectory()
	if err != nil {
		glog.Error("SetImportDirectory fail", err.Error())
		return err
	}
	err = service.SetExportDirectory()
	if err != nil {
		glog.Error("SetExportDirectory fail", err.Error())
		return err
	}
	err = service.AuthorizeUser()
	if err != nil {
		glog.Error("initConnect(): AuthorizeUser fail: ", err.Error())
		service.Close()
		return err
	}

	glog.Info("initConnect(): Init campus service complete")
	return nil
}

func (service *CampusService) exchange() {
	var ticker *time.Ticker
	var req CampusExchange
	var response []byte
	buf := make([]byte, RESPONSE_BUFFER_SIZE)
	for {
		switch {
		case service.connect == nil:
			glog.Info("exchange(): service.connect section")
			time.Sleep(500 * time.Millisecond)
			glog.Warning("exchange(): Connect to campus service is absent")
			if req.InProcess {
				req.InProcess = false
			}
		case req.InProcess && len(ticker.C) > 0:
			glog.Info("exchange(): timeout section")
			req.InProcess = false
			glog.Error("exchange(): Error timeout response from campus service")
			req.ChanError <- errors.New("Error timeout response from campus service")
		case !req.InProcess && len(service.chReq) > 0:
			glog.Info("exchange(): request section")
			req = <-service.chReq
			// Apend header
			header := make([]byte, DATA_HEADER_LENGTH)
			binary.BigEndian.PutUint32(header, uint32(len(req.Data)))
			req.Data = append(header[:], req.Data[:]...)
			ticker = time.NewTicker(time.Second * CAMPUS_SERVICE_TIMEOUT)
			n, err := service.connect.Write(req.Data)
			glog.Infof("exchange(): write bytes=[%v] data=%v[%s] error=[%v]", n, req.Data[:DATA_HEADER_LENGTH], req.Data[DATA_HEADER_LENGTH:], err)
			if err != nil {
				glog.Error("exchange(): Write to campus service server failed:", err.Error())
				service.Close()
				req.InProcess = false
				req.ChanError <- err
			}
			response = make([]byte, 0, RESPONSE_BUFFER_SIZE)
			glog.Info("exchange(): exit request section")
		case req.InProcess:
			glog.Info("exchange(): read section")
			glog.Info("exchange(): read header")
			header := make([]byte, DATA_HEADER_LENGTH)
			n, err := service.connect.Read(header)
			if err != io.EOF && err != nil {
				glog.Error(err)
				glog.Info("exchange(): exit read section")
				service.Close()
				req.InProcess = false
				req.ChanError <- err
			}
			if n != DATA_HEADER_LENGTH {
				glog.Infof("exchange(): incomplete header size=[%v]", n)
				glog.Info("exchange(): exit read section")
				service.Close()
				req.InProcess = false
				req.ChanError <- err
			}
			dataLen := int(binary.BigEndian.Uint32(header))
			glog.Infof("exchange(): in header setted data size=[%v]", dataLen)
			glog.Infof("exchange(): start read data", dataLen)
			for len(response) < dataLen {
				n, err = service.connect.Read(buf)
				if err != io.EOF && err != nil {
					glog.Error(err)
					glog.Info("exchange(): exit read section")
					service.Close()
					req.InProcess = false
					req.ChanError <- err
				}
				response = append(response, buf[:n]...)
				glog.Infof("exchange(): read in progress bytes=[%v] data=[%s]", len(response), response)
			}
			glog.Infof("exchange(): read from socket finished")
			glog.Infof("exchange(): response bytes=[%v] data=[%s]", dataLen, response)
			glog.Info("exchange(): exit read section")
			ticker.Stop()
			req.InProcess = false
			req.ChanResponse <- response
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// Make request to campus service server
func (service *CampusService) MakeRequest(request string) (*string, error) {
	glog.Infof("MakeRequest(): Request:%s\n", request)

	chres := make(chan []byte)
	cherr := make(chan error)
	defer close(chres)
	defer close(cherr)

	service.chReq <- CampusExchange{[]byte(request), chres, cherr, true}
	for {
		select {
		case response := <-chres:
			glog.Infof("MakeRequest(): Response:%s\n", response)
			value := string(response)
			return &value, nil
		case err := <-cherr:
			glog.Error("MakeRequest(): Error read from campus service:", err.Error())
			return nil, fmt.Errorf("Error read from campus service: %v", err.Error())
		}
	}
}

func (service *CampusService) GetSamReaderStatus() error {
	reply, err := service.MakeRequest("<Request><GetSamReaderStatus/></Request>")
	if err != nil {
		glog.Error("GetSamReaderStatus(): Request GetSamReaderStatus to campus service failed:", err.Error())
		return err
	}
	response := ResponseSamReaderStatus{}
	err = xml.Unmarshal([]byte(*reply), &response)
	if err != nil {
		glog.Errorf("GetSamReaderStatus(): Error parse response of request GetSamReaderStatus from campus service: %v", err)
		return err
	}
	if response.Status.Code != "0" {
		glog.Warningf("GetSamReaderStatus(): Response status code of request GetSamReaderStatus:%s Message:%s", response.Status.Code, response.Status.Message)
		return errors.New(response.Status.Message)
	}
	if response.SamReaderStatus.Code != "10" && response.SamReaderStatus.Code != "11" {
		glog.Warningf("GetSamReaderStatus(): Error SamReader status. Code:%s Message:%s", response.SamReaderStatus.Code, response.SamReaderStatus.Message)
		return errors.New(response.SamReaderStatus.Message)
	}
	return nil
}

func (service *CampusService) GetContactlessReaderStatus() error {
	reply, err := service.MakeRequest("<Request><GetContactlessReaderStatus/></Request>")
	if err != nil {
		glog.Error("GetContactlessReaderStatus(): Request GetContactlessReaderStatus to campus service failed:", err.Error())
		return err
	}
	response := ResponseContactlessReaderStatus{}
	err = xml.Unmarshal([]byte(*reply), &response)
	if err != nil {
		glog.Errorf("GetContactlessReaderStatus(): Error parse response of request GetContactlessReaderStatus from campus service: %v", err)
		return err
	}
	if response.Status.Code != "0" {
		glog.Warningf("GetContactlessReaderStatus(): Response status code of request GetContactlessReaderStatus:%s Message:%s", response.Status.Code, response.Status.Message)
		return errors.New(response.Status.Message)
	}
	if response.ContactlessReaderStatus.Code != "10" && response.ContactlessReaderStatus.Code != "11" {
		glog.Warningf("GetContactlessReaderStatus(): Error SamReader status. Code:%s Message:%s", response.ContactlessReaderStatus.Code, response.ContactlessReaderStatus.Message)
		return errors.New(response.ContactlessReaderStatus.Message)
	}
	return nil
}

func (service *CampusService) SetSamReader() error {
	reply, err := service.MakeRequest(`<Request><SetSamReader Device="` + service.conf.SamReaderDevice + `" Type="` + service.conf.SamReaderType + `"/></Request>`)
	if err != nil {
		glog.Error("Request SetSamReader to campus service failed:", err.Error())
		return err
	}
	response := ResponseStatus{}
	err = xml.Unmarshal([]byte(*reply), &response)
	if err != nil {
		glog.Errorf("Error parse response of request SetSamReader from campus service: %v", err)
		return err
	}
	if response.Status.Code != "0" {
		glog.Errorf("Response status code of request SetSamReader:%s Message:%s", response.Status.Code, response.Status.Message)
		return errors.New(response.Status.Message)
	}
	return nil
}

func (service *CampusService) SetContactlessReader() error {
	reply, err := service.MakeRequest(`<Request><SetContactlessReader Device="` + service.conf.ContactlessReaderDevice + `" Type="` + service.conf.ContactlessReaderType + `"/></Request>`)
	if err != nil {
		glog.Error("Request SetContactlessReader to campus service failed:", err.Error())
		return err
	}
	response := ResponseStatus{}
	err = xml.Unmarshal([]byte(*reply), &response)
	if err != nil {
		glog.Errorf("Error parse response of request SetContactlessReader from campus service: %v", err)
		return err
	}
	if response.Status.Code != "0" {
		glog.Errorf("Response status code of request SetContactlessReader:%s Message:%s", response.Status.Code, response.Status.Message)
		return errors.New(response.Status.Message)
	}
	return nil
}
func (service *CampusService) AuthorizeUser() error {
	reply, err := service.MakeRequest(`<Request><AuthorizeUser Login="` + service.conf.SamUser + `" Password="` + service.conf.SamPassword + `"/></Request>`)
	if err != nil {
		glog.Error("Request AuthorizeUser to campus service failed:", err.Error())
		return err
	}
	response := ResponseStatus{}
	err = xml.Unmarshal([]byte(*reply), &response)
	if err != nil {
		glog.Errorf("Error parse response of request AuthorizeUser from campus service: %v", err)
		return err
	}
	if response.Status.Code != "0" {
		glog.Errorf("Response status code of request AuthorizeUser:%s Message:%s", response.Status.Code, response.Status.Message)
		return errors.New(response.Status.Message)
	}
	return nil
}

func (service *CampusService) SetAuthorizParams() error {
	reply, err := service.MakeRequest(`<Request><SetAuthorizParams Host="` + service.conf.AuthHostAddress + `" Port="` + service.conf.AuthHostPort + `" UriTemplate="` + service.conf.AuthHostURITemplate + `"/></Request>`)
	if err != nil {
		glog.Error("Request AuthorizParams to campus service failed:", err.Error())
		return err
	}
	response := ResponseStatus{}
	err = xml.Unmarshal([]byte(*reply), &response)
	if err != nil {
		glog.Errorf("Error parse response of request AuthorizParams from campus service: %v", err)
		return err
	}
	if response.Status.Code != "0" {
		glog.Errorf("Response status code of request AuthorizParams:%s Message:%s", response.Status.Code, response.Status.Message)
		return errors.New(response.Status.Message)
	}
	return nil
}

func (service *CampusService) SetServerParams() error {
	reply, err := service.MakeRequest(`<Request><SetServerParams Host="` + service.conf.CardholdersHostAddress + `" Port="` + service.conf.CardholdersHostPort + `" UriTemplate="` + service.conf.CardholdersHostURITemplate + `"/></Request>`)
	if err != nil {
		glog.Error("Request SetServerParams to campus service failed:", err.Error())
		return err
	}
	response := ResponseStatus{}
	err = xml.Unmarshal([]byte(*reply), &response)
	if err != nil {
		glog.Errorf("Error parse response of request SetServerParams from campus service: %v", err)
		return err
	}
	if response.Status.Code != "0" {
		glog.Errorf("Response status code of request SetServerParams:%s Message:%s", response.Status.Code, response.Status.Message)
		return errors.New(response.Status.Message)
	}
	return nil
}

func (service *CampusService) WBRequest(accnum string) error {
	reply, err := service.MakeRequest(`<Request><WBRequest CampusNumber="` + accnum + `" WBCode="White"/></Request>`)
	if err != nil {
		glog.Errorf("Request WBRequest to campus service for %v failed:%v", accnum, err.Error())
		return err
	}
	response := ResponseStatus{}
	err = xml.Unmarshal([]byte(*reply), &response)
	if err != nil {
		glog.Errorf("Error parse response of request WBRequest from campus service: %v", err)
		return err
	}
	if response.Status.Code != "0" {
		glog.Errorf("Response status code of request WBRequest:%s Message:%s", response.Status.Code, response.Status.Message)
		return errors.New(response.Status.Message)
	}
	return nil
}

func (service *CampusService) GetCardBalance(card string) (*CardBalance, error) {
	reply, err := service.MakeRequest(`<Request><GetCardBalance CampusNumber="` + card + `"/></Request>`)
	if err != nil {
		glog.Errorf("Request GetCardBalance to campus service for %v failed:%v", card, err.Error())
		return nil, err
	}
	response := ResponseCardBalance{}
	err = xml.Unmarshal([]byte(*reply), &response)
	if err != nil {
		glog.Errorf("Error parse response of request GetCardBalance from campus service: %v", err)
		return nil, err
	}
	if response.Status.Code != "0" {
		glog.Errorf("Response status code of request GetCardBalance:%s Message:%s", response.Status.Code, response.Status.Message)
		return nil, errors.New(response.Status.Message)
	}
	//	glog.Infof("Requested GetCardBalance from campus service for %v, balance is %v", card, )
	return &response.CardBalance, nil
}

func (service *CampusService) GetCardholderData(card string) (*ResponseCardholderData, error) {
	reply, err := service.MakeRequest(`<Request><RequestData CampusNumber="` + card + `"/></Request>`)
	if err != nil {
		glog.Error("Request RequestData to campus service is fail:", err.Error())
		return nil, err
	}
	response := ResponseCardholderData{}
	err = xml.Unmarshal([]byte(*reply), &response)
	if err != nil {
		glog.Errorf("Error parse response of request RequestData from campus service: %v", err)
		return nil, err
	}
	if response.Status.Code != "0" {
		glog.Errorf("Response status code of request RequestData:%s Message:%s", response.Status.Code, response.Status.Message)
		return nil, errors.New(response.Status.Message)
	}
	return &response, nil
}

func (service *CampusService) GetAuthorisData(card string, period int) ([]Transaction, error) {
	now := time.Now()
	reply, err := service.MakeRequest(`<Request><GetAuthorisData CampusNumber="` + card + `"` +
		` PeriodEnd="` + now.Format(CAMPUS_DATE_FORMAT) + `"` + ` PeriodStart="` + now.AddDate(0, 0, -period).Format(CAMPUS_DATE_FORMAT) + `"` +
		`/></Request>`)
	if err != nil {
		glog.Errorf("Request GetAuthorisData to campus service for %v failed:%v", card, err.Error())
		return nil, err
	}
	response := ResponseAuthorisData{}
	err = xml.Unmarshal([]byte(*reply), &response)
	if err != nil {
		glog.Errorf("Error parse response of request GetAuthorisData from campus service: %v", err)
		return nil, err
	}
	if response.Status.Code != "0" {
		glog.Errorf("Response status code of request GetAuthorisData:%s Message:%s", response.Status.Code, response.Status.Message)
		return nil, errors.New(response.Status.Message)
	}
	return response.AuthorisData.Transactions, nil
}

func (service *CampusService) SetImportDirectory() error {
	if service.conf.ImportDirectory != "" {
		glog.Warning("importDirectory parameter is empty. Request SetImportDirectory skiped")
	}
	reply, err := service.MakeRequest(`<Request><SetImportDirectory Path="` + service.conf.ImportDirectory + `"/></Request>`)
	if err != nil {
		glog.Error("Request SetImportDirectory to campus service failed:", err.Error())
		return err
	}
	response := ResponseStatus{}
	err = xml.Unmarshal([]byte(*reply), &response)
	if err != nil {
		glog.Errorf("Error parse response of request SetImportDirectory from campus service: %v", err)
		return err
	}
	if response.Status.Code != "0" {
		glog.Errorf("Response status code of request SetImportDirectory:%s Message:%s", response.Status.Code, response.Status.Message)
		return errors.New(response.Status.Message)
	}
	return nil
}

func (service *CampusService) SetExportDirectory() error {
	if service.conf.ExportDirectory != "" {
		glog.Warning("ExportDirectory parameter is empty. Request SetExportDirectory skiped")
	}
	reply, err := service.MakeRequest(`<Request><SetExportDirectory Path="` + service.conf.ExportDirectory + `"/></Request>`)
	if err != nil {
		glog.Error("Request SetExportDirectory to campus service failed:", err.Error())
		return err
	}
	response := ResponseStatus{}
	err = xml.Unmarshal([]byte(*reply), &response)
	if err != nil {
		glog.Errorf("Error parse response of request SetExportDirectory from campus service: %v", err)
		return err
	}
	if response.Status.Code != "0" {
		glog.Errorf("Response status code of request SetExportDirectory:%s Message:%s", response.Status.Code, response.Status.Message)
		return errors.New(response.Status.Message)
	}
	return nil
}

func (service *CampusService) FinancialOperation(card, operation, sum, taskid string) error {
	reply, err := service.MakeRequest(fmt.Sprintf("<Request><FinancialOperation CampusNumber=%q OperCode=%q Summa=%q TaskUniqueNumber=%q/></Request>", card, operation, sum, taskid))
	if err != nil {
		glog.Error("Request FinancialOperation to campus service failed:", err.Error())
		return err
	}
	response := ResponseStatus{}
	err = xml.Unmarshal([]byte(*reply), &response)
	if err != nil {
		glog.Errorf("Error parse response of request FinancialOperation from campus service: %v", err)
		return err
	}
	if response.Status.Code != "0" {
		glog.Errorf("Response status code of request FinancialOperation:%s Message:%s", response.Status.Code, response.Status.Message)
		return errors.New(response.Status.Message)
	}
	return nil
}
