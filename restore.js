require('dotenv').config();
const { MongoClient } = require('mongodb');
const fs = require('fs');
const path = require('path');
const readline = require('readline');

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

function question(query) {
  return new Promise(resolve => rl.question(query, resolve));
}

function getAllBackups(dir, fileList = []) {
  const files = fs.readdirSync(dir);
  
  files.forEach(file => {
    const filePath = path.join(dir, file);
    const stat = fs.statSync(filePath);
    
    if (stat.isDirectory()) {
      getAllBackups(filePath, fileList);
    } else if (file.endsWith('.json')) {
      fileList.push(filePath);
    }
  });
  
  return fileList;
}

async function listBackups() {
  const backupDir = process.env.BACKUP_DIR || './backups';
  
    if (!fs.existsSync(backupDir)) {
    console.log('El directorio de backups no existe:', backupDir);
    return [];
  }

  const files = getAllBackups(backupDir)
    .sort()
    .reverse(); // Más recientes primero

  return files;
}

async function selectBackup() {
  const backups = await listBackups();
  
  if (backups.length === 0) {
    console.log('No se encontraron backups disponibles');
    return null;
  }

  console.log('\nBackups disponibles:\n');
  backups.forEach((file, index) => {
    const stats = fs.statSync(file);
    const size = (stats.size / 1024).toFixed(2);
    const fileName = path.basename(file);
    const relPath = path.relative(process.env.BACKUP_DIR || './backups', file);
    console.log(`${index + 1}. ${relPath} (${size} KB) - ${stats.mtime.toLocaleString('es-MX')}`);
  });

  console.log('');
  const answer = await question('Selecciona el número del backup a restaurar (o "q" para salir): ');
  
  if (answer.toLowerCase() === 'q') {
    return null;
  }

  const index = parseInt(answer) - 1;
  
  if (isNaN(index) || index < 0 || index >= backups.length) {
    console.log('Selección inválida');
    return null;
  }

  return backups[index];
}

async function restoreBackup(backupPath) {
  const backupData = JSON.parse(fs.readFileSync(backupPath, 'utf8'));
  
  console.log(`\nInformación del backup:`);
  console.log(`   Base de datos: ${backupData.database}`);
  console.log(`   Fecha: ${new Date(backupData.timestamp).toLocaleString('es-MX')}`);
  console.log(`   Colecciones:`);
  
  for (const [collectionName, collectionData] of Object.entries(backupData.collections)) {
    console.log(`   - ${collectionName}: ${collectionData.count} documentos`);
  }

  console.log('');
  const confirm = await question('¿Confirmar restauración? Esto eliminará los datos actuales (s/n): ');
  
  if (confirm.toLowerCase() !== 's') {
    console.log('Restauración cancelada');
    return;
  }

  const mongoUri = process.env.MONGO_URI;
  const dbName = process.env.DB_NAME || backupData.database;
  
  const client = new MongoClient(mongoUri);
  
  try {
    console.log('\nConectando a MongoDB...');
    await client.connect();
    console.log('Conectado exitosamente');
    
    const db = client.db(dbName);
    
    for (const [collectionName, collectionData] of Object.entries(backupData.collections)) {
      console.log(`\nProcesando ${collectionName}...`);
      const collection = db.collection(collectionName);
      
      // Eliminar datos existentes
      const deleteResult = await collection.deleteMany({});
      console.log(`   ${deleteResult.deletedCount} documentos eliminados`);
      
      // Insertar nuevos datos
      if (collectionData.count > 0) {
        await collection.insertMany(collectionData.documents);
        console.log(`   ${collectionData.count} documentos restaurados`);
      } else {
        console.log(`   Colección vacía, no hay documentos para restaurar`);
      }
    }
    
    console.log('\nRestauración completada exitosamente');
    
  } catch (error) {
    console.error('\nError durante la restauración:', error.message);
    throw error;
  } finally {
    await client.close();
    console.log('Conexión cerrada');
  }
}

async function main() {
  console.log('═══════════════════════════════════════');
  console.log('   RESTAURACIÓN DE BACKUP MONGODB   ');
  console.log('═══════════════════════════════════════\n');

  try {
    const backupPath = await selectBackup();
    
    if (!backupPath) {
      console.log('\nHasta luego');
      rl.close();
      return;
    }

    await restoreBackup(backupPath);
    
  } catch (error) {
    console.error('\nError:', error.message);
  } finally {
    rl.close();
  }
}

main();