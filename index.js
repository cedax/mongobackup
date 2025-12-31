require('dotenv').config();

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { MongoClient } = require('mongodb');

// Configuración desde variables de entorno
const MONGO_URI = process.env.MONGO_URI
const DB_NAME = process.env.DB_NAME
const BACKUP_DIR = process.env.BACKUP_DIR
const DAYS_TO_KEEP = parseInt(process.env.DAYS_TO_KEEP, 10)

// Crear directorio de backups si no existe
if (!fs.existsSync(BACKUP_DIR)) {
    fs.mkdirSync(BACKUP_DIR, { recursive: true });
}

function generateBackupPath() {
    const now = new Date();
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    const hours = String(now.getHours()).padStart(2, '0');
    const minutes = String(now.getMinutes()).padStart(2, '0');
    const seconds = String(now.getSeconds()).padStart(2, '0');
    const hash = crypto.randomBytes(4).toString('hex');

    const dateDir = path.join(BACKUP_DIR, year.toString(), month, day);
    
    if (!fs.existsSync(dateDir)) {
        fs.mkdirSync(dateDir, { recursive: true });
    }

    const fileName = `${hours}_${minutes}_${seconds}_${hash}.json`;
    return path.join(dateDir, fileName);
}

async function createBackup() {
    const backupPath = generateBackupPath();

    const client = new MongoClient(MONGO_URI);

    try {
        await client.connect();
        console.log('Conectado a MongoDB Atlas');

        const db = client.db(DB_NAME);
        const collections = await db.listCollections().toArray();

        const backup = {
            database: DB_NAME,
            timestamp: new Date().toISOString(),
            collections: {}
        };

        for (const collInfo of collections) {
            const collName = collInfo.name;
            console.log(`Respaldando coleccion: ${collName}`);

            const collection = db.collection(collName);
            const documents = await collection.find({}).toArray();

            backup.collections[collName] = {
                count: documents.length,
                documents: documents
            };
        }

        fs.writeFileSync(backupPath, JSON.stringify(backup, null, 2));
        console.log(`Backup creado: ${backupPath}`);
        console.log(`Total de colecciones: ${collections.length}`);

        return backupPath;
    } catch (error) {
        console.error('Error al crear backup:', error);
        throw error;
    } finally {
        await client.close();
    }
}

function cleanOldBackups(daysToKeep = 30) {
    try {
        const cutoffDate = new Date();
        cutoffDate.setDate(cutoffDate.getDate() - daysToKeep);

        let deletedCount = 0;

        // Función recursiva para buscar y eliminar archivos antiguos
        function processDirectory(dir) {
            if (!fs.existsSync(dir)) return;
            
            const entries = fs.readdirSync(dir, { withFileTypes: true });
            
            for (const entry of entries) {
                const fullPath = path.join(dir, entry.name);
                
                if (entry.isDirectory()) {
                    processDirectory(fullPath);
                    // Eliminar directorio si está vacío
                    const remaining = fs.readdirSync(fullPath);
                    if (remaining.length === 0) {
                        fs.rmdirSync(fullPath);
                        console.log(`Directorio vacío eliminado: ${fullPath}`);
                    }
                } else if (entry.name.endsWith('.json')) {
                    const stats = fs.statSync(fullPath);
                    if (stats.mtime < cutoffDate) {
                        fs.unlinkSync(fullPath);
                        console.log(`Eliminado: ${fullPath}`);
                        deletedCount++;
                    }
                }
            }
        }

        processDirectory(BACKUP_DIR);

        if (deletedCount === 0) {
            console.log('No hay backups antiguos para eliminar');
        } else {
            console.log(`Eliminados ${deletedCount} backup(s) antiguo(s)`);
        }
    } catch (error) {
        console.error('Error al limpiar backups antiguos:', error.message);
    }
}

function getBackupStats() {
    try {
        let totalSize = 0;
        let fileCount = 0;

        function countFiles(dir) {
            if (!fs.existsSync(dir)) return;
            
            const entries = fs.readdirSync(dir, { withFileTypes: true });
            
            for (const entry of entries) {
                const fullPath = path.join(dir, entry.name);
                
                if (entry.isDirectory()) {
                    countFiles(fullPath);
                } else if (entry.name.endsWith('.json')) {
                    const stats = fs.statSync(fullPath);
                    totalSize += stats.size;
                    fileCount++;
                }
            }
        }

        countFiles(BACKUP_DIR);

        const totalSizeMB = (totalSize / (1024 * 1024)).toFixed(2);

        console.log('\nEstadisticas de backups:');
        console.log(`   Total de backups: ${fileCount}`);
        console.log(`   Espacio utilizado: ${totalSizeMB} MB`);
        console.log(`   Ubicación: ${path.resolve(BACKUP_DIR)}`);
    } catch (error) {
        console.error('Error al obtener estadísticas:', error.message);
    }
}

async function main() {
    try {
        console.log('Iniciando proceso de backup...');
        console.log(`Base de datos: ${DB_NAME}`);
        console.log(`Directorio: ${BACKUP_DIR}`);
        console.log(`Retencion: ${DAYS_TO_KEEP} dias\n`);

        await createBackup();

        console.log('\nLimpiando backups antiguos...');
        cleanOldBackups(DAYS_TO_KEEP);

        getBackupStats();

        console.log('\nProceso completado exitosamente');
    } catch (error) {
        console.error('\nError en el proceso:', error.message);
        process.exit(1);
    }
}

main();