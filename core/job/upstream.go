package job

type SpecUpstream struct {
	upstreamNames []string
	httpUpstreams []*HTTPUpstreams
}

func NewSpecUpstream(upstreamNames []string, httpUpstreams []*HTTPUpstreams) *SpecUpstream {
	return &SpecUpstream{upstreamNames: upstreamNames, httpUpstreams: httpUpstreams}
}

func (s SpecUpstream) UpstreamNames() []string {
	return s.upstreamNames
}

func (s SpecUpstream) HTTPUpstreams() []*HTTPUpstreams {
	return s.httpUpstreams
}

type HTTPUpstreams struct {
	name    string
	url     string
	headers map[string]string
	params  map[string]string
}

func (h HTTPUpstreams) Name() string {
	return h.name
}

func (h HTTPUpstreams) URL() string {
	return h.url
}

func (h HTTPUpstreams) Headers() map[string]string {
	return h.headers
}

func (h HTTPUpstreams) Params() map[string]string {
	return h.params
}

func NewHTTPUpstream(name string, url string, headers map[string]string, params map[string]string) *HTTPUpstreams {
	return &HTTPUpstreams{name: name, url: url, headers: headers, params: params}
}
