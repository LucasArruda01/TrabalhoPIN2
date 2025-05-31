import mysql.connector
import pandas as pd
from mysql.connector import Error
from datetime import date, datetime

dataframe = pd.DataFrame()

def connect_to_database():
    """Tenta conectar ao banco de dados e retorna o objeto de conexão."""
    try:
        connection = mysql.connector.connect(
            host=	'127.0.0.1'	,          
            user=	'root'	,        
            password=	''	,      
            database=	'biblioteca'	  
        )
        if connection.is_connected():
            print("Conexão com o banco de dados estabelecida com sucesso.")
            return connection
    except Error as e:
        print(f"Erro ao conectar ao banco de dados MySQL: {e}")
        print("Verifique se o serviço MySQL está rodando e se as credenciais (host, user, password, database) estão corretas.")
        return None

def insert_data(connection, nome, nascimento, sexo, rua, numero, bairro, cidade, email, telefone):
    """Insere dados na tabela pessoas."""
    cursor = None
    try:
        cursor = connection.cursor()
        query = """INSERT INTO pessoas 
                     (nome, nascimento, sexo, rua, numero, bairro, cidade, email, telefone) 
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        values = (nome, nascimento, sexo, rua, numero, bairro, cidade, email, telefone)
        cursor.execute(query, values)
        connection.commit()
        print("Pessoa inserida com sucesso.")
    except Error as e:
        print(f"Erro ao inserir pessoa: {e}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()

def update_data(connection, id, campos_para_atualizar):
    """Atualiza dados na tabela pessoas."""
    cursor = None
    try:
        if not campos_para_atualizar:
            print("Nenhum campo foi modificado para atualização.")
            return
        cursor = connection.cursor()
        set_clause = ", ".join([f"{campo} = %s" for campo in campos_para_atualizar])
        query = f"UPDATE pessoas SET {set_clause} WHERE id = %s"
        values = tuple(campos_para_atualizar.values()) + (id,)
        cursor.execute(query, values)
        connection.commit()
        print(f"Cadastro da pessoa ID {id} atualizado com sucesso!")
    except Error as e:
        print(f"Erro ao atualizar pessoa: {e}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()

def delete_data(connection, id):
    """Deleta dados da tabela pessoas."""
    cursor = None
    try:
        cursor = connection.cursor()
        query = "DELETE FROM pessoas WHERE id = %s"
        values = (id,)
        cursor.execute(query, values)
        connection.commit()
        print("Pessoa deletada com sucesso.")
    except Error as e:
        print(f"Erro ao deletar pessoa: {e}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()

def read_data(connection, nome_busca):
    """Lê dados da tabela pessoas filtrando por nome."""
    cursor = None
    try:
        cursor = connection.cursor()
        query = "SELECT id, nome, nascimento, sexo, rua, numero, bairro, cidade, email, telefone FROM pessoas WHERE nome LIKE %s"
        valor_busca = f"%{nome_busca}%"
        cursor.execute(query, (valor_busca,))
        rows = cursor.fetchall()
        if not rows:
            print(f"Nenhuma pessoa encontrada com nome similar a '{nome_busca}'.")
            return
        colunas = ["ID", "Nome", "Nascimento", "Sexo", "Rua", "Numero", "Bairro", "Cidade", "Email", "Telefone"]
        dataframe_local = pd.DataFrame(rows, columns=colunas)
        print(dataframe_local.to_string())
    except Error as e:
        print(f"Erro ao buscar pessoas: {e}")
    finally:
        if cursor:
            cursor.close()


def registrar_emprestimo(connection, pessoa_id, livro_id, data_emprestimo, data_devolucao_prevista):
    """Registra um novo empréstimo na tabela emprestimos."""
    cursor = None
    try:
        cursor = connection.cursor()
        query = """INSERT INTO emprestimos 
                     (pessoa_id, livro_id, data_emprestimo, data_devolucao_prevista) 
                 VALUES (%s, %s, %s, %s)"""
        values = (pessoa_id, livro_id, data_emprestimo, data_devolucao_prevista)
        cursor.execute(query, values)
        connection.commit()
        print("Empréstimo registrado com sucesso.")
    except Error as e:
        print(f"Erro ao registrar empréstimo: {e}")
        if e.errno == 1452: # Foreign key constraint fails
            print("Erro: ID da Pessoa ou ID do Livro não encontrado nas tabelas correspondentes.")
        elif connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()

def registrar_devolucao(connection, emprestimo_id, data_devolucao_real):
    """Registra a data de devolução real para um empréstimo existente."""
    cursor = None
    try:
        cursor = connection.cursor()
        # Verifica se o empréstimo existe e ainda não foi devolvido
        check_query = "SELECT id FROM emprestimos WHERE id = %s AND data_devolucao_real IS NULL"
        cursor.execute(check_query, (emprestimo_id,))
        resultado_check = cursor.fetchone()
        
        if resultado_check is None:
             # Verifica se o empréstimo existe mas já foi devolvido
            check_exists_query = "SELECT id FROM emprestimos WHERE id = %s"
            cursor.execute(check_exists_query, (emprestimo_id,))
            if cursor.fetchone():
                 print(f"Erro: Empréstimo ID {emprestimo_id} já consta como devolvido.")
            else:
                 print(f"Erro: Empréstimo ID {emprestimo_id} não encontrado.")
            return False 

        query = "UPDATE emprestimos SET data_devolucao_real = %s WHERE id = %s"
        values = (data_devolucao_real, emprestimo_id)
        cursor.execute(query, values)
        connection.commit()
        print(f"Devolução do empréstimo ID {emprestimo_id} registrada com sucesso.")
        return True 
    except Error as e:
        print(f"Erro ao registrar devolução: {e}")
        if connection:
            connection.rollback()
        return False 
    finally:
        if cursor:
            cursor.close()

def ver_emprestimos(connection):
    """Exibe os empréstimos relacionando pessoas e livros."""
    cursor = None
    try:
        cursor = connection.cursor()
        # Query com JOIN para buscar nomes e títulos
        query = """SELECT 
                       e.id AS EmprestimoID,
                       p.nome AS NomePessoa, 
                       l.Nome AS TituloLivro, -- Usando 'Nome' para título, conforme informado
                       e.data_emprestimo AS DataEmprestimo, 
                       e.data_devolucao_prevista AS DevolucaoPrevista,
                       e.data_devolucao_real AS DevolucaoReal
                   FROM 
                       emprestimos e 
                   JOIN 
                       pessoas p ON e.pessoa_id = p.id 
                   JOIN 
                       livros l ON e.livro_id = l.id
                   ORDER BY e.data_emprestimo DESC; -- Ordena pelos mais recentes
                """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        if not rows:
            print("Nenhum empréstimo registrado encontrado.")
            return
            
        colunas = ["EmprestimoID", "Nome Pessoa", "Titulo Livro", "Data Emprestimo", "Devolucao Prevista", "Devolucao Real"]
        dataframe_local = pd.DataFrame(rows, columns=colunas)
        print("\n--- Lista de Empréstimos ---")
        print(dataframe_local.to_string())

    except Error as e:
        print(f"Erro ao buscar dados de empréstimos: {e}")
        if e.errno == 1146: 
             print(f"Erro específico: Tabela não encontrada (detalhes: {e.msg}). Verifique o nome da tabela de livros.")
        elif e.errno == 1054: 
             print(f"Erro específico: Coluna desconhecida (detalhes: {e.msg}). Verifique o nome da coluna de título na tabela de livros.")
    finally:
        if cursor:
            cursor.close()

def input_data(prompt):
    """Solicita uma data ao usuário e valida o formato AAAA-MM-DD."""
    while True:
        data_str = input(prompt + " (AAAA-MM-DD): ").strip()
        try:
            data_obj = datetime.strptime(data_str, '%Y-%m-%d').date()
            return data_obj
        except ValueError:
            print("Formato de data inválido. Use AAAA-MM-DD.")

def main():
    """Função principal do script."""
    connection = connect_to_database()

    if connection:
        while True:
            print("\n--- Menu Biblioteca ---")
            print("1. Inserir Pessoa") 
            print("2. Atualizar Pessoa") 
            print("3. Deletar Pessoa")  
            print("4. Buscar Pessoa por Nome")
            print("5. Ver Todos os Empréstimos") 
            print("6. Registrar Novo Empréstimo") 
            print("7. Registrar Devolução por Pessoa")
            print("8. Sair") 
            choice = input("Digite o número da opção: ").strip()

            if choice == '1': 
                try:
                    nome_input = input("Digite o nome: ")
                    nascimento_input = input_data("Digite o nascimento") 
                    sexo_input = input("Digite o sexo (M/F): ").upper()
                    if sexo_input not in ('M', 'F'):
                         print("Sexo inválido. Use M ou F.")
                         continue
                    print("--- Endereço e Contato ---") 
                    rua_input = input("Digite a rua: ")
                    numero_input = int(input("Digite o número: "))
                    bairro_input = input("Digite o bairro: ")
                    cidade_input = input("Digite a cidade: ")
                    email_input = input("Digite o email: ")
                    telefone_input = input("Digite o telefone: ")
                    insert_data(connection, nome_input, nascimento_input, sexo_input, 
                                rua_input, numero_input, bairro_input, cidade_input, 
                                email_input, telefone_input)
                except ValueError:
                    print("Erro: O valor digitado para 'Número' não é um número inteiro válido.") 
                except Exception as e:
                    print(f"Ocorreu um erro inesperado durante a inserção: {e}")

            elif choice == '2':
                cursor = None
                try:
                    id_input = int(input("Digite o ID da pessoa para atualizar: "))
                    cursor = connection.cursor(dictionary=True)
                    query_select = "SELECT * FROM pessoas WHERE id = %s"
                    cursor.execute(query_select, (id_input,))
                    dados_atuais = cursor.fetchone()
                    cursor.close()
                    cursor = None
                    if not dados_atuais:
                        print(f"Erro: Pessoa com ID {id_input} não encontrada.")
                        continue
                    print(f"\n--- Atualizando dados para ID: {id_input} ---")
                    print("(Pressione Enter para manter o valor atual)")
                    campos_para_atualizar = {}
                    def pedir_atualizacao(campo, label, valor_atual, tipo=str):
                        novo_valor_str = input(f"{label} (atual: {valor_atual}): ").strip()
                        if novo_valor_str:
                            try:
                                novo_valor = tipo(novo_valor_str)
                                if campo == 'sexo':
                                    novo_valor = novo_valor.upper()
                                    if novo_valor not in ('M', 'F'):
                                        print("Valor inválido para Sexo (M/F). Mantendo o atual.")
                                        return False

                                if campo == 'nascimento':
                                    try:
                                        datetime.strptime(str(novo_valor_str), '%Y-%m-%d')
                                    except ValueError:
                                        print("Formato de data inválido para Nascimento (AAAA-MM-DD). Mantendo o atual.")
                                        return False
                                campos_para_atualizar[campo] = novo_valor
                                return True
                            except ValueError:
                                print(f"Valor inválido para {label}. Mantendo o atual.")
                                return False
                        return False
                    pedir_atualizacao('nome',       'Nome',       dados_atuais['nome'])
                    pedir_atualizacao('nascimento', 'Nascimento (AAAA-MM-DD)', dados_atuais['nascimento'])
                    pedir_atualizacao('sexo',       'Sexo (M/F)', dados_atuais['sexo'])
                    pedir_atualizacao('rua',        'Rua',        dados_atuais['rua'])
                    pedir_atualizacao('numero',     'Número',     dados_atuais['numero'], tipo=int)
                    pedir_atualizacao('bairro',     'Bairro',     dados_atuais['bairro'])
                    pedir_atualizacao('cidade',     'Cidade',     dados_atuais['cidade'])
                    pedir_atualizacao('email',      'Email',      dados_atuais['email'])
                    pedir_atualizacao('telefone',   'Telefone',   dados_atuais['telefone'])
                    if campos_para_atualizar:
                        update_data(connection, id_input, campos_para_atualizar)
                    else:
                        print("Nenhum campo foi alterado.")
                except ValueError:
                    print("Erro: ID inválido. Digite um número.")
                except Error as e:
                    print(f"Erro durante a operação de atualização: {e}")
                except Exception as e:
                     print(f"Ocorreu um erro inesperado: {e}")
                finally:
                    if cursor:
                        cursor.close()

            elif choice == '3':
                try:
                    id_input = int(input("Digite o ID da pessoa para deletar: "))
                    confirmacao = input(f"Tem certeza que deseja deletar o registro ID {id_input}? (s/N): ").strip().lower()
                    if confirmacao == 's':
                        delete_data(connection, id_input)
                    else:
                        print("Operação cancelada.")
                except ValueError:
                     print("Erro: ID inválido. Digite um número.")
            
            elif choice == '4':
                nome_busca = input("Digite o nome (ou parte do nome) da pessoa para buscar: ").strip()
                if nome_busca:
                    print(f"\nBuscando pessoas com nome similar a '{nome_busca}':") 
                    read_data(connection, nome_busca)
                else:
                    print("Nome para busca não pode ser vazio.")

            elif choice == '5': 
                ver_emprestimos(connection)

            elif choice == '6': 
                print("\n--- Registrar Novo Empréstimo ---")
                try:
                    pessoa_id_input = int(input("Digite o ID da Pessoa: "))
                    livro_id_input = int(input("Digite o ID do Livro: "))
                    data_emprestimo_input = input_data("Digite a Data de Empréstimo")
                    data_prevista_input = input_data("Digite a Data de Devolução Prevista")

                    if data_prevista_input < data_emprestimo_input:
                        print("Erro: A data de devolução prevista não pode ser anterior à data de empréstimo.")
                        continue
                        
                    registrar_emprestimo(connection, pessoa_id_input, livro_id_input, 
                                       data_emprestimo_input, data_prevista_input)
                except ValueError:
                    print("Erro: ID da Pessoa ou ID do Livro deve ser um número inteiro.")
                except Exception as e:
                    print(f"Ocorreu um erro inesperado ao registrar empréstimo: {e}")

            elif choice == '7': 
                print("\n--- Registrar Devolução por Pessoa ---")
                cursor = None 
                try:
                    pessoa_id_input = int(input("Digite o ID da Pessoa que está devolvendo: "))
                    cursor = connection.cursor()
                    query_pendentes = """SELECT e.id, l.Nome, e.data_emprestimo, e.data_devolucao_prevista 
                                         FROM emprestimos e 
                                         JOIN livros l ON e.livro_id = l.id 
                                         WHERE e.pessoa_id = %s AND e.data_devolucao_real IS NULL
                                         ORDER BY e.id;"""
                    cursor.execute(query_pendentes, (pessoa_id_input,))
                    pendentes = cursor.fetchall()
                    
                    if not pendentes:
                        print(f"Nenhum empréstimo pendente encontrado para a Pessoa ID {pessoa_id_input}.")
                        continue 
                    
                    print(f"\nEmpréstimos pendentes para a Pessoa ID {pessoa_id_input}:")
                    df_pendentes = pd.DataFrame(pendentes, columns=["EmprestimoID", "Titulo Livro", "Data Emprestimo", "Devolucao Prevista"])
                    print(df_pendentes.to_string())
                
                    emprestimo_id_input = int(input("Digite o ID do Empréstimo a ser devolvido da lista acima: "))
                    
                    ids_pendentes_validos = [item[0] for item in pendentes]
                    if emprestimo_id_input not in ids_pendentes_validos:
                        print("Erro: O ID do Empréstimo fornecido não está na lista de pendentes para esta pessoa.")
                        continue
                        
                    data_devolucao_input = input_data("Digite a Data de Devolução Real")
                    
                    registrar_devolucao(connection, emprestimo_id_input, data_devolucao_input)
                    
                except ValueError:
                    print("Erro: O ID da Pessoa ou do Empréstimo deve ser um número inteiro.")
                except Error as e:
                     print(f"Erro de banco de dados ao buscar/registrar devolução: {e}")
                except Exception as e:
                    print(f"Ocorreu um erro inesperado ao registrar devolução: {e}")
                finally:
                    if cursor:
                        cursor.close() 

            elif choice == '8': 
                print("Saindo...")
                break
            else:
                print("Opção inválida. Tente novamente.")

        if connection and connection.is_connected():
            connection.close()
            print("Conexão com o banco de dados fechada.")
    else:
        print("Não foi possível conectar ao banco de dados. Encerrando o programa.")


if __name__ == "__main__":
    main()

