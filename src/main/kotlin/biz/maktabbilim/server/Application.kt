package biz.maktabbilim.server

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.cio.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.callloging.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.cors.routing.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.websocket.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.*
import java.net.URI
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

@Serializable
data class HealthResponse(val status: String, val service: String, val databaseConnected: Boolean)

@Serializable
data class EduContentItem(
    val id: Long,
    val category: String,
    val grade: Int,
    val subject: String,
    val title: String,
    val tgFileId: String,
    val fileSizeMb: Double
)

@Serializable
data class CreateContentRequest(
    val category: String,
    val grade: Int,
    val subject: String,
    val title: String,
    val tgFileId: String,
    val fileSizeMb: Double = 0.0
)

@Serializable
data class ChatMessageDto(
    val id: Long = 0,
    val channelId: String,
    val senderName: String,
    val senderId: String,
    val text: String,
    val createdAt: String = ""
)

@Serializable
data class SendMessageRequest(
    val channelId: String,
    val senderName: String,
    val senderId: String,
    val text: String
)

@Serializable
data class AiAskRequest(
    val userId: String,
    val grade: Int = 10,
    val question: String
)

@Serializable
data class AiAskResponse(
    val answer: String,
    val fromCache: Boolean
)

@Serializable
data class TelegramFileUrlResponse(
    val downloadUrl: String
)

object DatabaseManager {
    var dataSource: HikariDataSource? = null
        private set

    fun init(rawDatabaseUrl: String?) {
        if (rawDatabaseUrl.isNullOrBlank()) {
            println("WARNING: DATABASE_URL is not set. Running without PostgreSQL persistence.")
            return
        }
        val jdbcUrl: String
        val username: String?
        val password: String?

        if (rawDatabaseUrl.startsWith("jdbc:")) {
            jdbcUrl = rawDatabaseUrl
            username = null
            password = null
        } else {
            val uri = URI(rawDatabaseUrl.replace("postgres://", "postgresql://"))
            val userInfo = uri.userInfo?.split(":", limit = 2)
            username = userInfo?.getOrNull(0)
            password = userInfo?.getOrNull(1)
            val port = if (uri.port > 0) ":${uri.port}" else ""
            val query = if (!uri.query.isNullOrBlank()) "?${uri.query}" else "?sslmode=require"
            jdbcUrl = "jdbc:postgresql://${uri.host}$port${uri.path}$query"
        }

        val config = HikariConfig().apply {
            this.jdbcUrl = jdbcUrl
            if (username != null) this.username = username
            if (password != null) this.password = password
            maximumPoolSize = 5
            minimumIdle = 1
            connectionTimeout = 10_000
        }
        dataSource = HikariDataSource(config)
        createTables()
    }

    private fun createTables() {
        val ds = dataSource ?: return
        ds.connection.use { conn ->
            conn.createStatement().use { st ->
                st.execute(
                    """
                    CREATE TABLE IF NOT EXISTS edu_contents (
                        id BIGSERIAL PRIMARY KEY,
                        category VARCHAR(64) NOT NULL,
                        grade INT NOT NULL DEFAULT 0,
                        subject VARCHAR(100) NOT NULL,
                        title VARCHAR(255) NOT NULL,
                        tg_file_id VARCHAR(255) NOT NULL,
                        file_size_mb DOUBLE PRECISION DEFAULT 0.0,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    );
                    """.trimIndent()
                )
                st.execute(
                    """
                    CREATE TABLE IF NOT EXISTS chat_messages (
                        id BIGSERIAL PRIMARY KEY,
                        channel_id VARCHAR(128) NOT NULL,
                        sender_name VARCHAR(120) NOT NULL,
                        sender_id VARCHAR(120) NOT NULL,
                        text TEXT NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    );
                    """.trimIndent()
                )
                st.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_chat_channel_id
                    ON chat_messages(channel_id, id DESC);
                    """.trimIndent()
                )
                st.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ai_faq_cache (
                        id BIGSERIAL PRIMARY KEY,
                        question_norm TEXT UNIQUE NOT NULL,
                        answer TEXT NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    );
                    """.trimIndent()
                )
                // Eski xato keshlarni tozalash
                st.execute("DELETE FROM ai_faq_cache WHERE answer LIKE 'Kechirasiz, javob olishda xatolik%'")
            }
        }
    }
}

fun main() {
    val port = System.getenv("PORT")?.toIntOrNull() ?: 8080
    DatabaseManager.init(System.getenv("DATABASE_URL"))

    embeddedServer(Netty, port = port, host = "0.0.0.0") {
        module()
    }.start(wait = true)
}

private fun extractTextFromGeminiJson(root: JsonObject): String? {
    root["output_text"]?.jsonPrimitive?.contentOrNull?.takeIf { it.isNotBlank() }?.let { return it.trim() }

    root["output"]?.jsonArray?.firstOrNull()?.jsonObject?.get("text")?.jsonPrimitive?.contentOrNull
        ?.takeIf { it.isNotBlank() }?.let { return it.trim() }

    root["steps"]?.jsonArray?.forEach { el ->
        val txt = el.jsonObject["model_output"]?.jsonObject?.get("text")?.jsonPrimitive?.contentOrNull
        if (!txt.isNullOrBlank()) return txt.trim()
    }

    root["candidates"]?.jsonArray?.firstOrNull()
        ?.jsonObject?.get("content")?.jsonObject
        ?.get("parts")?.jsonArray?.firstOrNull()
        ?.jsonObject?.get("text")?.jsonPrimitive?.contentOrNull
        ?.takeIf { it.isNotBlank() }?.let { return it.trim() }

    return null
}

fun Application.module() {
    val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }
    val httpClient = HttpClient(CIO) {
        install(io.ktor.client.plugins.contentnegotiation.ContentNegotiation) {
            json(json)
        }
    }

    val botToken = System.getenv("TELEGRAM_BOT_TOKEN") ?: ""
    val adminSecret = System.getenv("ADMIN_SECRET") ?: "maktabbilim_admin_2026"
    val geminiApiKey = (System.getenv("GEMINI_API_KEY") ?: "").trim()

    val rooms = ConcurrentHashMap<String, MutableSet<WebSocketSession>>()

    install(ContentNegotiation) { json(json) }
    install(CallLogging)
    install(CORS) {
        anyHost()
        allowHeader(HttpHeaders.ContentType)
        allowHeader(HttpHeaders.Authorization)
    }
    install(WebSockets) {
        pingPeriod = Duration.ofSeconds(20)
        timeout = Duration.ofSeconds(30)
    }

    routing {
        get("/") {
            call.respond(
                HealthResponse(
                    status = "ok",
                    service = "MaktabBilim.biz Ktor Server",
                    databaseConnected = DatabaseManager.dataSource != null
                )
            )
        }

        get("/health") {
            call.respond(
                HealthResponse(
                    status = "ok",
                    service = "MaktabBilim.biz Ktor Server",
                    databaseConnected = DatabaseManager.dataSource != null
                )
            )
        }

        // 1. Ta'lim kontentlari (Darsliklar, Video Darslar, Kutubxona)
        get("/api/v1/content") {
            val category = call.request.queryParameters["category"] ?: "darsliklar"
            val grade = call.request.queryParameters["grade"]?.toIntOrNull()
            val ds = DatabaseManager.dataSource
            if (ds == null) {
                call.respond(emptyList<EduContentItem>())
                return@get
            }
            val items = mutableListOf<EduContentItem>()
            ds.connection.use { conn ->
                val sql = if (grade != null) {
                    "SELECT id, category, grade, subject, title, tg_file_id, file_size_mb FROM edu_contents WHERE category = ? AND grade = ? ORDER BY id DESC LIMIT 100"
                } else {
                    "SELECT id, category, grade, subject, title, tg_file_id, file_size_mb FROM edu_contents WHERE category = ? ORDER BY grade ASC, id DESC LIMIT 100"
                }
                conn.prepareStatement(sql).use { ps ->
                    ps.setString(1, category)
                    if (grade != null) ps.setInt(2, grade)
                    ps.executeQuery().use { rs ->
                        while (rs.next()) {
                            items.add(
                                EduContentItem(
                                    id = rs.getLong("id"),
                                    category = rs.getString("category"),
                                    grade = rs.getInt("grade"),
                                    subject = rs.getString("subject"),
                                    title = rs.getString("title"),
                                    tgFileId = rs.getString("tg_file_id"),
                                    fileSizeMb = rs.getDouble("file_size_mb")
                                )
                            )
                        }
                    }
                }
            }
            call.respond(items)
        }

        post("/api/v1/content") {
            val secret = call.request.headers["X-Admin-Secret"] ?: ""
            if (secret != adminSecret) {
                call.respond(HttpStatusCode.Unauthorized, mapOf("error" to "Ruxsat yo'q"))
                return@post
            }
            val req = call.receive<CreateContentRequest>()
            val ds = DatabaseManager.dataSource ?: return@post call.respond(
                HttpStatusCode.ServiceUnavailable,
                mapOf("error" to "Baza ulanmagan")
            )
            ds.connection.use { conn ->
                conn.prepareStatement(
                    "INSERT INTO edu_contents(category, grade, subject, title, tg_file_id, file_size_mb) VALUES (?, ?, ?, ?, ?, ?)"
                ).use { ps ->
                    ps.setString(1, req.category)
                    ps.setInt(2, req.grade)
                    ps.setString(3, req.subject)
                    ps.setString(4, req.title)
                    ps.setString(5, req.tgFileId)
                    ps.setDouble(6, req.fileSizeMb)
                    ps.executeUpdate()
                }
            }
            call.respond(HttpStatusCode.Created, mapOf("status" to "saved"))
        }

        // 2. Telegram yopiq kanalidagi fayl uchun xavfsiz yuklash linkini olish
        get("/api/v1/telegram/file-url") {
            val fileId = call.request.queryParameters["file_id"]
            if (fileId.isNullOrBlank() || botToken.isBlank()) {
                call.respond(HttpStatusCode.BadRequest, mapOf("error" to "file_id yoki TELEGRAM_BOT_TOKEN mavjud emas"))
                return@get
            }
            val tgResp: JsonObject = httpClient.get("https://api.telegram.org/bot$botToken/getFile?file_id=$fileId").body()
            val ok = tgResp["ok"]?.jsonPrimitive?.booleanOrNull == true
            val filePath = tgResp["result"]?.jsonObject?.get("file_path")?.jsonPrimitive?.contentOrNull
            if (!ok || filePath.isNullOrBlank()) {
                call.respond(HttpStatusCode.NotFound, mapOf("error" to "Telegram fayli topilmadi"))
                return@get
            }
            val url = "https://api.telegram.org/file/bot$botToken/$filePath"
            call.respond(TelegramFileUrlResponse(downloadUrl = url))
        }

        // 3. Chat & Guruhlar: oxirgi 30 ta xabarni yuklash
        get("/api/v1/messages") {
            val channelId = call.request.queryParameters["channel_id"] ?: "global_chat"
            val limit = (call.request.queryParameters["limit"]?.toIntOrNull() ?: 30).coerceIn(1, 50)
            val ds = DatabaseManager.dataSource
            if (ds == null) {
                call.respond(emptyList<ChatMessageDto>())
                return@get
            }
            val list = mutableListOf<ChatMessageDto>()
            ds.connection.use { conn ->
                conn.prepareStatement(
                    "SELECT id, channel_id, sender_name, sender_id, text, created_at FROM chat_messages WHERE channel_id = ? ORDER BY id DESC LIMIT ?"
                ).use { ps ->
                    ps.setString(1, channelId)
                    ps.setInt(2, limit)
                    ps.executeQuery().use { rs ->
                        while (rs.next()) {
                            list.add(
                                ChatMessageDto(
                                    id = rs.getLong("id"),
                                    channelId = rs.getString("channel_id"),
                                    senderName = rs.getString("sender_name"),
                                    senderId = rs.getString("sender_id"),
                                    text = rs.getString("text"),
                                    createdAt = rs.getString("created_at") ?: ""
                                )
                            )
                        }
                    }
                }
            }
            call.respond(list.reversed())
        }

        post("/api/v1/messages") {
            val req = call.receive<SendMessageRequest>()
            if (req.text.isBlank()) {
                call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Bo'sh xabar"))
                return@post
            }
            var savedId = 0L
            DatabaseManager.dataSource?.connection?.use { conn ->
                conn.prepareStatement(
                    "INSERT INTO chat_messages(channel_id, sender_name, sender_id, text) VALUES (?, ?, ?, ?) RETURNING id"
                ).use { ps ->
                    ps.setString(1, req.channelId)
                    ps.setString(2, req.senderName)
                    ps.setString(3, req.senderId)
                    ps.setString(4, req.text.trim())
                    ps.executeQuery().use { rs ->
                        if (rs.next()) savedId = rs.getLong(1)
                    }
                }
            }
            val dto = ChatMessageDto(
                id = savedId,
                channelId = req.channelId,
                senderName = req.senderName,
                senderId = req.senderId,
                text = req.text.trim()
            )
            val payload = json.encodeToString(dto)
            rooms[req.channelId]?.forEach { session ->
                runCatching { session.send(Frame.Text(payload)) }
            }
            call.respond(HttpStatusCode.Created, dto)
        }

        webSocket("/ws/chat/{channelId}") {
            val channelId = call.parameters["channelId"] ?: "global_chat"
            val set = rooms.computeIfAbsent(channelId) { ConcurrentHashMap.newKeySet() }
            set.add(this)
            try {
                for (frame in incoming) {
                    if (frame is Frame.Text) {
                        val raw = frame.readText()
                        set.forEach { peer ->
                            if (peer != this) runCatching { peer.send(Frame.Text(raw)) }
                        }
                    }
                }
            } finally {
                set.remove(this)
            }
        }

        // 4. AI Repetitor (FAQ kesh + Fallback zanjiri: generateContent va interactions)
        post("/api/v1/ai/ask") {
            val req = call.receive<AiAskRequest>()
            val norm = req.question.trim().lowercase()
            if (norm.isBlank()) {
                call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Savol bo'sh"))
                return@post
            }

            // Avval keshdan tekshirish
            DatabaseManager.dataSource?.connection?.use { conn ->
                conn.prepareStatement("SELECT answer FROM ai_faq_cache WHERE question_norm = ? LIMIT 1").use { ps ->
                    ps.setString(1, norm)
                    ps.executeQuery().use { rs ->
                        if (rs.next()) {
                            val cached = rs.getString("answer")
                            if (!cached.startsWith("Kechirasiz, javob olishda xatolik")) {
                                call.respond(AiAskResponse(answer = cached, fromCache = true))
                                return@post
                            }
                        }
                    }
                }
            }

            if (geminiApiKey.isBlank()) {
                call.respond(
                    AiAskResponse(
                        answer = "AI Repetitor hozircha sozlanmoqda (serverda GEMINI_API_KEY kiritilmagan).",
                        fromCache = false
                    )
                )
                return@post
            }

            val prompt = """
                Sen O'zbekiston maktablarining ${req.grade}-sinf o'quvchilari uchun mehribon AI Repetitorsan (MaktabBilim.biz Milliy Intellekti).
                O'zbek tilida, sodda, bosqichma-bosqich va tushunarli tushuntir. Murakkab LaTeX belgilarsiz oddiy formulalar bilan yoz.
                Foydalanuvchi so'rovi: ${req.question}
            """.trimIndent()

            var finalAnswer: String? = null
            var lastError = ""

            // 1-usul: interactions API (AQ.Ab8... kalitlari va yangi modellar uchun)
            for (modelName in listOf("gemini-2.5-flash", "gemini-3.7-flash", "gemini-2.0-flash")) {
                try {
                    val interBody = buildJsonObject {
                        put("model", modelName)
                        put("input", prompt)
                    }
                    val httpResp = httpClient.post("https://generativelanguage.googleapis.com/v1beta/interactions?key=$geminiApiKey") {
                        contentType(ContentType.Application.Json)
                        setBody(interBody)
                    }
                    val rawText = httpResp.bodyAsText()
                    if (httpResp.status.isSuccess()) {
                        val parsed = extractTextFromGeminiJson(json.parseToJsonElement(rawText).jsonObject)
                        if (!parsed.isNullOrBlank()) {
                            finalAnswer = parsed
                            break
                        }
                    } else {
                        lastError += "[interactions/$modelName ${httpResp.status.value}] "
                    }
                } catch (e: Exception) {
                    lastError += "[interactions/$modelName: ${e.message}] "
                }
            }

            // 2-usul: generateContent API (AIza... kalitlari uchun)
            if (finalAnswer == null) {
                for (modelName in listOf("gemini-2.5-flash", "gemini-2.0-flash")) {
                    try {
                        val bodyJson = buildJsonObject {
                            putJsonArray("contents") {
                                addJsonObject {
                                    putJsonArray("parts") {
                                        addJsonObject { put("text", prompt) }
                                    }
                                }
                            }
                        }
                        val httpResp = httpClient.post("https://generativelanguage.googleapis.com/v1beta/models/$modelName:generateContent?key=$geminiApiKey") {
                            contentType(ContentType.Application.Json)
                            setBody(bodyJson)
                        }
                        val rawText = httpResp.bodyAsText()
                        if (httpResp.status.isSuccess()) {
                            val parsed = extractTextFromGeminiJson(json.parseToJsonElement(rawText).jsonObject)
                            if (!parsed.isNullOrBlank()) {
                                finalAnswer = parsed
                                break
                            }
                        } else {
                            lastError += "[generateContent/$modelName ${httpResp.status.value}: $rawText] "
                        }
                    } catch (e: Exception) {
                        lastError += "[generateContent/$modelName: ${e.message}] "
                    }
                }
            }

            val resolvedAnswer = finalAnswer
            if (resolvedAnswer != null) {
                DatabaseManager.dataSource?.connection?.use { conn ->
                    conn.prepareStatement(
                        "INSERT INTO ai_faq_cache(question_norm, answer) VALUES (?, ?) ON CONFLICT (question_norm) DO UPDATE SET answer = EXCLUDED.answer"
                    ).use { ps ->
                        ps.setString(1, norm)
                        ps.setString(2, resolvedAnswer)
                        ps.executeUpdate()
                    }
                }
                call.respond(AiAskResponse(answer = resolvedAnswer, fromCache = false))
            } else {
                println("AI ERROR: $lastError")
                call.respond(
                    AiAskResponse(
                        answer = "AI javobida xatolik: $lastError",
                        fromCache = false
                    )
                )
            }
        }
    }
}
