const fs = require('fs');
const path = require('path');
const xlsx = require('xlsx');
const mysql = require('mysql2/promise');

function formatCurrencyToDecimal(value) {
    if (!value) {
      console.error('Valor inválido ou indefinido:', value);
      return 0;
    }

    let numericValue = value.replace(/R\$\s?/g, '').replace(/\./g, '').replace(/,/g, '.');
    return parseFloat(numericValue);
  }

function excelDateToMySQL(serial) {
    if (!serial || isNaN(serial)) {
      console.error('Número serial inválido:', serial);
      return null;
    }
  
    try {
      const excelEpoch = new Date(1900, 0, 1);
      const adjustedDate = new Date(excelEpoch.getTime() + (serial - 1) * 86400000);
      const year = adjustedDate.getFullYear();
      const month = String(adjustedDate.getMonth() + 1).padStart(2, '0');
      const day = String(adjustedDate.getDate()).padStart(2, '0');
      return `${year}-${month}-${day}`;
    } catch (error) {
      console.error('Erro ao converter número serial para data:', error);
      return null;
    }
  }
  
async function readExcelAndPersistToDB(filePath, tableName, dbConfig) {
  try {
    if (!fs.existsSync(filePath)) {
      throw new Error(`O arquivo ${filePath} não foi encontrado.`);
    }

    const workbook = xlsx.readFile(filePath);
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];

    const jsonData = xlsx.utils.sheet_to_json(worksheet);

    const dataList = jsonData.map((row) => {

      return [
        row['contrato'],
        row['cpf'],
        row['origem'],
        (row['D8']),
        excelDateToMySQL(row['venc']),
      ];
    });

    const connection = await mysql.createConnection(dbConfig);
    
    const query = `INSERT INTO ${tableName} (contrato, cpf, origem, valorDesconto, dataCompetencia) VALUES ?`;
    await connection.query(query, [dataList]);

    console.log(`Dados inseridos com sucesso na tabela ${tableName}.`);

    await connection.end();
  } catch (error) {
    console.error(`Erro ao processar o arquivo Excel: ${error.message}`);
  }
}

const dbConfig = {
  host: 'database-dev.ckqeyskgbzkh.us-east-2.rds.amazonaws.com',
  user: 'dbuser_devcrm',
  password: 'J3VG2Mo9kPje',
  database: 'crm',
};

const filePath = path.join(__dirname, 'Pasta11.xlsx');

const tableName = 'tbconciliacaotodavida';

readExcelAndPersistToDB(filePath, tableName, dbConfig);
