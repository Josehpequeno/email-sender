package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/smtp"
	"os"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	SMTP struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		Email    string `yaml:"email"`
		Password string `yaml:"password"`
	} `yaml:"smtp"`

	Limits struct {
		DailyEmailLimit int `yaml:"daily_limit"`
		DelaySeconds    int `yaml:"delay_seconds"`
		BatchSize       int `yaml:"batch_size"`
		Workers         int `yaml:"workers"`
	} `yaml:"limits"`

	Email struct {
		Subject  string `yaml:"subject"`
		Template string `yaml:"template"`
	} `yaml:"email"`
}

type Aluno struct {
	Campus             string `json:"campus"`
	Curso              string `json:"curso"`
	Matricula          string `json:"matricula"`
	Nome               string `json:"nome"`
	EmailInstitucional string `json:"email_institucional"`
}

type Resultado struct {
	Timestamp time.Time `json:"timestamp"`
	Aluno     Aluno     `json:"aluno"`
	Status    string    `json:"status"`
	Error     string    `json:"error,omitempty"`
}

type EmailSender struct {
	config     Config
	template   *template.Template
	resultados []Resultado
	mutex      sync.Mutex
	contadores struct {
		enviados int
		falhas   int
	}
}

func NewEmailSender(configPath string) (*EmailSender, error) {
	//carregar configuração do arquivo YAML
	config, err := loadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("erro ao carregar configuração: %v", err)
	}

	// carrega template
	tmpl, err := template.ParseFiles(config.Email.Template)
	if err != nil {
		return nil, fmt.Errorf("erro ao carregar template de email: %v", err)
	}

	return &EmailSender{
		config:     config,
		template:   tmpl,
		resultados: make([]Resultado, 0),
	}, nil
}

func loadConfig(configPath string) (Config, error) {
	var config Config

	file, err := os.Open(configPath)
	if err != nil {
		return config, err
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	err = decoder.Decode(&config)
	return config, err
}

func (es *EmailSender) CarregarAlunosCSV(csvPath string) ([]Aluno, error) {
	file, err := os.Open(csvPath)
	if err != nil {
		return nil, fmt.Errorf("erro ao abrir arquivo CSV: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ','          //delimitador
	reader.FieldsPerRecord = -1 //número variável de campos

	cabecalho, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("erro ao ler cabeçalho do CSV: %v", err)
	}

	//normalizar nomes das colunas
	colunas := make(map[string]int)
	for i, col := range cabecalho {
		colunas[strings.ToLower(strings.TrimSpace(col))] = i
	}

	//verificar colunas obrigatórias
	colunasObrigatorias := []string{"campus", "curso", "matricula", "nome", "email_institucional"}
	for _, col := range colunasObrigatorias {
		if _, ok := colunas[col]; !ok {
			return nil, fmt.Errorf("coluna obrigatória ausente no CSV: %s", col)
		}
	}

	var alunos []Aluno

	for {
		linha, err := reader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			continue //pular linhas com erro
		}

		aluno := Aluno{
			Campus:             obterValor(linha, colunas, "campus"),
			Curso:              obterValor(linha, colunas, "curso"),
			Matricula:          obterValor(linha, colunas, "matricula"),
			Nome:               obterValor(linha, colunas, "nome"),
			EmailInstitucional: obterValor(linha, colunas, "email_institucional"),
		}

		//validar email
		if aluno.EmailInstitucional == "" || !strings.Contains(aluno.EmailInstitucional, "@") {
			continue //pular emails inválidos
		}

		alunos = append(alunos, aluno)

	}
	log.Printf("📊 Carregados %d alunos do CSV", len(alunos))
	return alunos, nil
}

func obterValor(linha []string, colunas map[string]int, nomeColuna string) string {
	if idx, ok := colunas[nomeColuna]; ok && idx < len(linha) {
		return strings.TrimSpace(linha[idx])
	}
	return ""
}

func (es *EmailSender) GerarCorpoEmail(aluno Aluno) (string, error) {
	var builder strings.Builder

	err := es.template.Execute(&builder, aluno)
	if err != nil {
		return "", fmt.Errorf("erro ao executar template: %v", err)
	}

	return builder.String(), nil
}

func (es *EmailSender) EnviarEmail(aluno Aluno) Resultado {
	resultado := Resultado{
		Timestamp: time.Now(),
		Aluno:     aluno,
	}

	corpo, err := es.GerarCorpoEmail(aluno)
	if err != nil {
		resultado.Status = "falha"
		resultado.Error = fmt.Sprintf("erro ao gerar corpo do email: %v", err)
		es.registrarResultado(resultado)
		return resultado
	}

	//configurar mensagem
	mensagem := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/html; charset=\"UTF-8\"\r\n\r\n%s",
		es.config.SMTP.Email,
		aluno.EmailInstitucional,
		es.config.Email.Subject,
		corpo,
	)

	//configurar autenticação
	auth := smtp.PlainAuth("", es.config.SMTP.Email, es.config.SMTP.Password, es.config.SMTP.Host)
	// log.Println("Auth", auth)
	// log.Printf("Email de envio: %s", es.config.SMTP.Email)

	//enviar email
	enderecoSMTP := fmt.Sprintf("%s:%d", es.config.SMTP.Host, es.config.SMTP.Port)
	err = smtp.SendMail(enderecoSMTP, auth, es.config.SMTP.Email, []string{aluno.EmailInstitucional}, []byte(mensagem))
	if err != nil {
		resultado.Status = "falha"
		resultado.Error = fmt.Sprintf("erro ao enviar email: %v", err)
		es.registrarResultado(resultado)
		return resultado
	}
	resultado.Status = "sucesso"
	es.registrarResultado(resultado)
	return resultado
}

func (es *EmailSender) registrarResultado(resultado Resultado) {
	es.mutex.Lock()
	defer es.mutex.Unlock()

	es.resultados = append(es.resultados, resultado)

	if resultado.Status == "sucesso" {
		es.contadores.enviados++
		log.Printf("✅ Email enviado para %s", resultado.Aluno.EmailInstitucional)
	} else {
		es.contadores.falhas++
		log.Printf("❌ Falha ao enviar email para %s: %s", resultado.Aluno.EmailInstitucional, resultado.Error)
	}
}

func (es *EmailSender) EnviarEmailsEmLote(alunos []Aluno) {
	total := len(alunos)
	log.Printf("🚀 Iniciando envio para %d alunos", total)

	canalAlunos := make(chan Aluno, total)

	//preencher canal de alunos
	for _, aluno := range alunos {
		canalAlunos <- aluno
	}

	close(canalAlunos)

	var wg sync.WaitGroup

	//iniciar workers
	for i := 0; i < es.config.Limits.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for aluno := range canalAlunos {
				//verificar limite diário
				es.mutex.Lock()
				if es.contadores.enviados >= es.config.Limits.DailyEmailLimit {
					es.mutex.Unlock()
					log.Printf("⚠️ Limite diário de %d emails atingido. Worker %d encerrando.", es.config.Limits.DailyEmailLimit, workerID)
					return
				}
				es.mutex.Unlock()

				es.EnviarEmail(aluno)

				//delay entre email
				time.Sleep(time.Duration(es.config.Limits.DelaySeconds))
			}
		}(i)
	}

	wg.Wait()

	log.Printf("📈 Envio concluído: %d enviados, %d falhas", es.contadores.enviados, es.contadores.falhas)
}

// salvarResultados salva resultados em json
func (es *EmailSender) SalvarResultados(caminho string) error {
	es.mutex.Lock()
	defer es.mutex.Unlock()

	if caminho == "" {
		caminho = fmt.Sprintf("resultados_emails_%s.json", time.Now().Format("20060102_150405"))
	}

	file, err := os.Create(caminho)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", " ")

	err = encoder.Encode(es.resultados)
	if err != nil {
		return err
	}

	log.Printf("💾 Resultados salvos em: %s", caminho)
	return nil
}

func (es *EmailSender) GerarRelatorio() {
	es.mutex.Lock()
	defer es.mutex.Unlock()

	log.Printf("\n📊 RELATÓRIO FINAL")
	log.Printf("====================")
	log.Printf("Total de processados: %d", len(es.resultados))
	log.Printf("Emails enviados: %d", es.contadores.enviados)
	log.Printf("Falhas: %d", es.contadores.falhas)
	log.Printf("Taxa de sucesso: %.2f%%",
		float64(es.contadores.enviados)/float64(len(es.resultados))*100)
}

func main() {
	// inicializar sender
	sender, err := NewEmailSender("config.yaml")
	if err != nil {
		log.Fatalf("Erro ao inicializar EmailSender: %v", err)
	}

	// carregar alunos do CSV
	alunos, err := sender.CarregarAlunosCSV("alunos.csv")
	if err != nil {
		log.Fatalf("Erro ao carregar alunos do CSV: %v", err)
	}

	if len(alunos) == 0 {
		log.Fatalf("Nenhum aluno válido encontrado no CSV.")
	}

	// enviar emails em lote
	sender.EnviarEmailsEmLote(alunos)

	// salvar resultados
	err = sender.SalvarResultados("")
	if err != nil {
		log.Printf("Erro ao salvar resultados: %v", err)
	}

	// gerar relatório final
	sender.GerarRelatorio()
}
