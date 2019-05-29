package http

import (
	"html/template"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	logWriter "github.com/kdamarla/empnearme/log"

	domain "github.com/kdamarla/empnearme/domain"
)

var templates = template.Must(template.ParseFiles("templates/list.html", "templates/single.html"))

//LcaHandler handles all car http requests
type LcaHandler struct {
	LcaRepo domain.LcaRepo
	Log     logWriter.Writer
}

//Handler for all incoming http requests
type Handler struct {
	LcaHandler LcaHandler
}

//Serve http at predecided port
func Serve(handler Handler) error {
	srv := &http.Server{
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("Connection", "close")
			url := "https://" + req.Host + req.URL.String()
			http.Redirect(w, req, url, http.StatusMovedPermanently)
		}),
	}
	http.Handle("/lca", handler.LcaHandler)
	return srv.ListenAndServeTLS("", "")
	//http.ListenAndServe(":8080", handler)
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
		return
	}
	http.Error(res, "Not Found", http.StatusNotFound)
}

func (lcaHandler LcaHandler) ServeHTTP(res http.ResponseWriter, req *http.Request) {

	p := req.URL.Query()
	zip := p.Get("z")
	radius, _ := strconv.Atoi(p.Get("r"))
	emp := strings.ToLower(p.Get("e"))
	minPay, _ := strconv.Atoi(p.Get("p"))
	h1After, _ := time.Parse("20060102", p.Get("d"))

	filter := domain.SearchCriteria{Radius: radius, Zipcode: zip, Employer: emp, MinimumPay: minPay, H1FiledAfter: h1After}

	lcas, err := lcaHandler.LcaRepo.Get(filter)
	if err != nil {
		lcaHandler.Log.Write(err)
	}

	//i think we need a vue js site served from server
	//res.Header().Set("Content-Type", "application/json")
	//res.WriteHeader(http.StatusOK)
	//todo - use html\template and render HTML on server side

	templates.ExecuteTemplate(res, "list.html", lcas)
	//json.NewEncoder(res).Encode(lcas)
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
