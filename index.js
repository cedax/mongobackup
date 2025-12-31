require('dotenv').config();

const fs = require('fs');
const path = require('path');
const { MongoClient } = require('mongodb');
const crypto = require('crypto');
const { exec } = require('child_process');
const { promisify } = require('util');

const execPromise = promisify(exec);

// Configuración desde variables de entorno
const MONGO_URI = process.env.MONGO_URI
const DB_NAME = process.env.DB_NAME
const BACKUP_DIR = process.env.BACKUP_DIR
const DAYS_TO_KEEP = parseInt(process.env.DAYS_TO_KEEP, 10)
const RCLONE_REMOTE = process.env.RCLONE_REMOTE
const ENABLE_SYNC = process.env.ENABLE_SYNC === 'true'

// Crear directorio de backups si no existe
if (!fs.existsSync(BACKUP_DIR)) {
    fs.mkdirSync(BACKUP_DIR, { recursive: true });
}

async function createBackup() {
    const now = new Date();

    // Formato de carpeta: AAAA/MM/DD
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');

    // Formato de archivo: HH_MM_SS_HASH.json
    const hours = String(now.getHours()).padStart(2, '0');
    const minutes = String(now.getMinutes()).padStart(2, '0');
    const seconds = String(now.getSeconds()).padStart(2, '0');
    const hash = crypto.randomBytes(4).toString('hex'); // 8 caracteres hexadecimales

    const dateFolder = path.join(BACKUP_DIR, year.toString(), month, day);
    const fileName = `${hours}_${minutes}_${seconds}_${hash}.json`;
    const backupPath = path.join(dateFolder, fileName);

    // Crear directorio de fecha si no existe
    if (!fs.existsSync(dateFolder)) {
        fs.mkdirSync(dateFolder, { recursive: true });
    }

    const client = new MongoClient(MONGO_URI);

    try {
        await client.connect();
        console.log('Conectado a MongoDB Atlas');

        const db = client.db(DB_NAME);
        const collections = await db.listCollections().toArray();

        const backup = {
            database: DB_NAME,
            timestamp: now.toISOString(),
            collections: {}
        };

        for (const collInfo of collections) {
            const collName = collInfo.name;
            console.log(`Respaldando colección: ${collName}`);

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

        // Función recursiva para explorar carpetas
        function scanDirectory(dir) {
            if (!fs.existsSync(dir)) return;

            const items = fs.readdirSync(dir);

            for (const item of items) {
                const itemPath = path.join(dir, item);
                const stats = fs.statSync(itemPath);

                if (stats.isDirectory()) {
                    // Recursivamente explorar subcarpetas
                    scanDirectory(itemPath);

                    // Eliminar carpeta si está vacía
                    if (fs.readdirSync(itemPath).length === 0) {
                        fs.rmdirSync(itemPath);
                        console.log(`Carpeta vacía eliminada: ${itemPath}`);
                    }
                } else if (stats.isFile() && item.endsWith('.json')) {
                    // Eliminar archivos JSON antiguos
                    if (stats.mtime < cutoffDate) {
                        fs.unlinkSync(itemPath);
                        console.log(`Eliminado: ${itemPath}`);
                        deletedCount++;
                    }
                }
            }
        }

        scanDirectory(BACKUP_DIR);

        if (deletedCount === 0) {
            console.log('No hay backups antiguos para eliminar');
        } else {
            console.log(`Eliminados ${deletedCount} backup(s) antiguo(s)`);
        }
    } catch (error) {
        console.error('Error al limpiar backups antiguos:', error.message);
    }
}

async function syncToDrive() {
    if (!ENABLE_SYNC) {
        console.log('Sincronización con Drive deshabilitada');
        return;
    }

    try {
        console.log('\nSincronizando con Google Drive...');

        const command = `rclone sync "${BACKUP_DIR}" "${RCLONE_REMOTE}" --transfers 4 --checkers 8`;
        const { stdout, stderr } = await execPromise(command);

        if (stderr) {
            console.log('Advertencias:', stderr);
        }

        console.log('Sincronización completada');
    } catch (error) {
        console.error('Error al sincronizar con Drive:', error.message);
        throw error;
    }
}

function getBackupStats() {
    try {
        let totalSize = 0;
        let backupCount = 0;

        // Función recursiva para contar archivos
        function scanDirectory(dir) {
            if (!fs.existsSync(dir)) return;

            const items = fs.readdirSync(dir);

            for (const item of items) {
                const itemPath = path.join(dir, item);
                const stats = fs.statSync(itemPath);

                if (stats.isDirectory()) {
                    scanDirectory(itemPath);
                } else if (stats.isFile() && item.endsWith('.json')) {
                    totalSize += stats.size;
                    backupCount++;
                }
            }
        }

        scanDirectory(BACKUP_DIR);

        const totalSizeMB = (totalSize / (1024 * 1024)).toFixed(2);

        console.log('\nEstadísticas de backups:');
        console.log(`   Total de backups: ${backupCount}`);
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
        console.log(`Retención: ${DAYS_TO_KEEP} días`);
        console.log(`Sync a Drive: ${ENABLE_SYNC ? 'Habilitado' : 'Deshabilitado'}\n`);

        const backupPath = await createBackup();

        console.log('\nLimpiando backups antiguos...');
        cleanOldBackups(DAYS_TO_KEEP);

        if (ENABLE_SYNC) {
            await syncToDrive();
        }

        getBackupStats();

        console.log('\nProceso completado exitosamente');
    } catch (error) {
        console.error('\nError en el proceso:', error.message);
        process.exit(1);
    }
}

main();