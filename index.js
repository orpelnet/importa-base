
const fs = require("fs");
const path = require("path");
const xlsx = require("xlsx");
const mysql = require("mysql2/promise");

async function readExcelAndPersistToDB(filePath, tableName, dbConfig, chunkSize = 10000) {
  try {
    // Verifica se o arquivo existe
    if (!fs.existsSync(filePath)) {
      throw new Error(`O arquivo ${filePath} não foi encontrado.`);
    }

    // Lê o arquivo Excel
    const workbook = xlsx.readFile(filePath);
    const sheetName = workbook.SheetNames[0]; // Seleciona a primeira aba
    console.log("Aba selecionada:", sheetName);

    const worksheet = workbook.Sheets[sheetName];

    // Converte a planilha para JSON
    const jsonData = xlsx.utils.sheet_to_json(worksheet, { header: 1 }); // Inclui a primeira linha (cabeçalhos)
    console.log(`Linhas totais na planilha: ${jsonData.length - 1}`);

    if (jsonData.length === 0) {
      throw new Error("A planilha está vazia.");
    }

    // Primeira linha contém os cabeçalhos
    const headers = jsonData[0].map((header) => header.replace(/\s+/g, "_").toLowerCase()); // Formata os cabeçalhos
    const rows = jsonData.slice(1); // Demais linhas são os dados

    // Conecta ao banco de dados
    const connection = await mysql.createConnection(dbConfig);

    // Cria a tabela dinamicamente
    console.log("Criando tabela no banco...");
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS ${tableName} (
        ${headers.map((col) => `\`${col}\` TEXT`).join(",")}
      );
    `;
    await connection.query(createTableQuery);
    console.log(`Tabela "${tableName}" criada com sucesso.`);

    // Remove dados antigos da tabela (se necessário)
    console.log(`Limpando a tabela "${tableName}"...`);
    await connection.query(`TRUNCATE TABLE ${tableName}`);

    // Insere os dados em chunks
    console.log("Iniciando inserções em lote...");
    for (let i = 0; i < rows.length; i += chunkSize) {
      const chunk = rows.slice(i, i + chunkSize); // Pega um lote de dados

      const placeholders = chunk
        .map(() => `(${headers.map(() => "?").join(",")})`)
        .join(",");
      const flattenedData = chunk.flat();

      const insertQuery = `INSERT INTO ${tableName} (${headers.join(",")}) VALUES ${placeholders}`;
      await connection.query(insertQuery, flattenedData);

      console.log(`Lote ${i / chunkSize + 1} inserido com sucesso.`);
    }

    console.log("Todos os dados foram inseridos com sucesso.");
    await connection.end();
  } catch (error) {
    console.error(`Erro ao processar o arquivo Excel: ${error.message}`);
  }
}

const dbConfig = {
  host: "database-dev.ckqeyskgbzkh.us-east-2.rds.amazonaws.com",
  user: "dbuser_devcrm",
  password: "J3VG2Mo9kPje",
  database: "crm",
};

const filePath = path.join(__dirname, "Planteck.xlsx");
const tableName = "tbconciliacaotodavidaTEMP";

readExcelAndPersistToDB(filePath, tableName, dbConfig);
