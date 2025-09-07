package natsbinding

type TLSConfig struct {
	CAFile             string
	CertFile           string
	KeyFile            string
	InsecureSkipVerify bool
}

type AuthConfig struct {
	// Choose one: NKey seed, JWT creds, user/pass, or token
	NkeySeedFile  string // e.g., ~/.nkeys/USER.seed
	UserCredsFile string // *.creds (JWT+NKey)
	Username      string
	Password      string
	Token         string
}

type Config struct {
	URLs          string   // e.g., "nats://host:4222"
	StreamName    string   // e.g., "AMPY"
	Subjects      []string // e.g., []string{"ampy.>"}
	DurablePrefix string   // e.g., "bus"

	Auth AuthConfig
	TLS  *TLSConfig
}
