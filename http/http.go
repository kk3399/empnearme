package http

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	domain "github.com/kk3399/empnearme/domain"
	logWriter "github.com/kk3399/empnearme/log"
	"golang.org/x/crypto/acme/autocert"
)

const (
	inProd    = false
	robotsTXT = `User-agent: *
				 Disallow: / `
)

var templates = template.Must(template.ParseFiles("templates/list.html", "templates/single.html"))

//LcaHandler handles all car http requests
type LcaHandler struct {
	LcaRepo domain.LcaRepo
	Log     logWriter.Writer
}

//StaticHandler handles index.html
type StaticHandler struct{}

//EmpListHandler handles auto complete request on employer names
type EmpListHandler struct {
	LcaRepo domain.LcaRepo
}

//Handler for all incoming http requests
type Handler struct {
	LcaHandler     LcaHandler
	StaticHandler  StaticHandler
	EmpListHandler EmpListHandler
}

//Serve http at predecided port
func Serve(handler Handler) error {
	var srv *http.Server
	var m *autocert.Manager

	if inProd {
		m = &autocert.Manager{
			Prompt:     autocert.AcceptTOS,
			HostPolicy: autocert.HostWhitelist("www.h1bemployersearch.com"),
			Cache:      autocert.DirCache("/home/letsencrypt/"),
		}

		srv = makeHTTPServer()
		srv.Addr = ":443"
		srv.TLSConfig = &tls.Config{
			GetCertificate: m.GetCertificate,
		}

		go func() {
			if err := srv.ListenAndServeTLS("", ""); err != nil {
				handler.LcaHandler.Log.Error(err.Error())
			}
		}()
	}

	srv = makeHTTPServer()

	if m != nil {
		srv.Handler = m.HTTPHandler(srv.Handler)
	}

	srv.Addr = ":8080"
	srv.Handler = handler

	//http.Handle("/lca", handler.LcaHandler)
	//http.Handle("/", http.FileServer(http.Dir("./static")))

	return srv.ListenAndServe()
	//http.ListenAndServe(":8080", handler)
}

func makeHTTPServer() *http.Server {
	return &http.Server{
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
}

//StartProfiling the app
func (h Handler) StartProfiling() {
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
}

func (h Handler) ServeHTTP(res http.ResponseWriter, req *http.Request) {

	var head string

	head, req.URL.Path = shiftPath(req.URL.Path)
	if head == "lca" {
		h.LcaHandler.ServeHTTP(res, req)
	} else if head == "emps" {
		h.EmpListHandler.ServeHTTP(res, req)
	} else if head == "robots.txt" {
		res.Header().Set("Content-Type", "text/plain")
		res.WriteHeader(http.StatusOK)
		fmt.Fprint(res, robotsTXT)

	} else {
		h.StaticHandler.ServeHTTP(res, req)
	}

	//http.Error(res, "Not Found", http.StatusNotFound)
}

func (staticHandler StaticHandler) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	//http.ServeFile(res, req, req.URL.Path[1:])
	http.ServeFile(res, req, "index.html")
}

func (empListHandler EmpListHandler) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	p := req.URL.Query()
	has := p.Get("has")
	if len(has) > 4 {
		res.Header().Set("Content-Type", "application/json")
		res.WriteHeader(http.StatusOK)
		json.NewEncoder(res).Encode(empListHandler.LcaRepo.GetEmployerNames(has))
	}
}

func (lcaHandler LcaHandler) ServeHTTP(res http.ResponseWriter, req *http.Request) {

	p := req.URL.Query()
	zip := p.Get("z")
	job := p.Get("j")
	x, _ := strconv.Atoi(p.Get("x"))
	radius, _ := strconv.Atoi(p.Get("r"))
	emp := strings.ToLower(p.Get("e"))
	payMin, _ := strconv.Atoi(p.Get("ps"))
	payMax, _ := strconv.Atoi(p.Get("pe"))
	year, _ := strconv.Atoi(p.Get("y"))
	//h1After, _ := time.Parse("20060102", p.Get("d"))

	filter := domain.SearchCriteria{Radius: radius, Zipcode: zip, Employer: emp, PayMin: payMin, PayMax: payMax, H1Year: year, JobTitle: job}
	if x > 0 {
		filter.ExcludeH1Dependent = true
	}

	lcas, err := lcaHandler.LcaRepo.Get(filter)
	if err != nil {
		lcaHandler.Log.Write(err)
	}

	res.Header().Set("Content-Type", "application/json")
	res.WriteHeader(http.StatusOK)
	json.NewEncoder(res).Encode(lcas)
	//templates.ExecuteTemplate(res, "list.html", lcas)

}

// shiftPath splits off the first component of p, which will be cleaned of
// relative components before processing. head will never contain a slash and
// tail will always be a rooted path without trailing slash.
func shiftPath(p string) (head, tail string) {
	p = path.Clean("/" + p)
	i := strings.Index(p[1:], "/") + 1
	if i <= 0 {
		return p[1:], "/"
	}
	return p[1:i], p[i:]
}
