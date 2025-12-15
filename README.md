# IST-KVS: Key-Value Store com Suporte a Clientes Remotos

Este projeto consiste num servidor de armazenamento de pares chave-valor (Key-Value Store - KVS), permitindo interação com múltiplos processos clientes através de **Named Pipes (FIFOs)**.

## 📋 Sobre o Projeto

O **IST-KVS** é um servidor autónomo que gere uma tabela de dispersão (hashtable) concorrente capaz de gerir sessões de clientes remotos.

### Funcionalidades Principais
* **Gestão de Sessões:** Suporte para múltiplos clientes simultâneos, geridos por uma pool de tarefas gestoras.
* **Comunicação IPC:** Utilização de Named Pipes (FIFOs) para troca de pedidos, respostas e notificações entre processos.
* **Subscrições:** Os clientes podem subscrever (`SUBSCRIBE`) chaves específicas e receber notificações em tempo real sempre que o valor dessa chave é alterado por outra tarefa.
* **Concorrência:** Utilização de multithreading (pthreads), mutexes e semáforos (modelo produtor-consumidor) para gerir acessos à hashtable e pedidos de conexão.
* **Gestão de Sinais:** Tratamento do sinal `SIGUSR1` para desconexão graciosa de todos os clientes e limpeza de subscrições sem terminar o servidor.

---

## ⚙️ Arquitetura

O sistema funciona com base numa arquitetura Cliente-Servidor com os seguintes componentes:

1.  **Tarefa Anfitriã (Host Thread):** Escuta o *FIFO de Registo* por novos pedidos de conexão (`connect`). Quando um pedido chega, coloca-o num buffer partilhado.
2.  **Tarefas Gestoras (Worker Threads):** Consomem pedidos do buffer e estabelecem uma sessão dedicada com o cliente.
3.  **Canais de Comunicação:** Cada sessão de cliente utiliza 3 FIFOs exclusivos:
    * `req_pipe`: Para o cliente enviar comandos (Subscribe/Unsubscribe/Disconnect).
    * `resp_pipe`: Para o servidor enviar o resultado das operações.
    * `notif_pipe`: Para o servidor enviar atualizações assíncronas de chaves subscritas.

---

## Setup e Utilização

### Pré-requisitos
* GCC Compiler
* Ambiente Linux

### 1. Compilação
Para compilar o projeto (servidor e cliente), utiliza o `Makefile` fornecido na raiz do projeto:

```bash
make

[cite_start]Para limpar os ficheiros binários e pipes criados[cite: 167]:
```bash
make clean


