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
import io.ktor.websocket.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*
import java.net.URI
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import kotlin.random.Random

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
    val createdAt: String = "",
    val isRead: Boolean = false
)

@Serializable
data class SendMessageRequest(
    val channelId: String,
    val senderName: String,
    val senderId: String,
    val text: String
)

@Serializable
data class MarkReadRequest(
    val channelId: String,
    val userId: String,
    val lastReadId: Long
)

@Serializable
data class PresenceResponse(
    val userId: String,
    val online: Boolean,
    val lastSeenMs: Long
)

@Serializable
data class UserRegisterRequest(
    val userId: String = "",
    val name: String,
    val region: String,
    val district: String = "",
    val school: String = "",
    val grade: String = "",
    val avatar: String = "boy_01"
)

@Serializable
data class UserHeartbeatRequest(
    val userId: String,
    val region: String = ""
)

@Serializable
data class UserProfileDto(
    val userId: String,
    val name: String,
    val region: String,
    val district: String,
    val school: String,
    val grade: String,
    val avatar: String,
    val online: Boolean
)

@Serializable
data class RegionOnlineCountDto(
    val region: String,
    val onlineCount: Int
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
                    CREATE TABLE IF NOT EXISTS app_users (
                        user_id VARCHAR(16) PRIMARY KEY,
                        name VARCHAR(120) NOT NULL,
                        region VARCHAR(80) NOT NULL,
                        district VARCHAR(80) NOT NULL DEFAULT '',
                        school VARCHAR(120) NOT NULL DEFAULT '',
                        grade VARCHAR(32) NOT NULL DEFAULT '',
                        avatar VARCHAR(64) NOT NULL DEFAULT 'boy_01',
                        last_seen_ms BIGINT NOT NULL DEFAULT 0,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    );
                    """.trimIndent()
                )
                st.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_app_users_region
                    ON app_users(region, last_seen_ms DESC);
                    """.trimIndent()
                )
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
                    CREATE TABLE IF NOT EXISTS channel_reads (
                        channel_id VARCHAR(128) NOT NULL,
                        user_id VARCHAR(120) NOT NULL,
                        last_read_id BIGINT NOT NULL DEFAULT 0,
                        updated_at TIMESTAMPTZ DEFAULT NOW(),
                        PRIMARY KEY (channel_id, user_id)
                    );
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

    val stepsArr = root["steps"]?.jsonArray
    if (stepsArr != null) {
        for (el in stepsArr) {
            val txt = el.jsonObject["model_output"]?.jsonObject?.get("text")?.jsonPrimitive?.contentOrNull
            if (!txt.isNullOrBlank()) return txt.trim()
        }
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
    val onlineUsers = ConcurrentHashMap<String, Long>()
    val lastSeenUsers = ConcurrentHashMap<String, Long>()

    suspend fun broadcastToRoom(channelId: String, payload: String, exclude: WebSocketSession? = null) {
        val targetSessions = rooms[channelId] ?: return
        for (session in targetSessions) {
            if (session != exclude) {
                runCatching { session.send(Frame.Text(payload)) }
            }
        }
    }

    fun generate9DigitId(): String = (100_000_000 + Random.nextInt(900_000_000)).toString()

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

        // 0. Foydalanuvchi 9 xonali unikal ID + Ro'yxatdan o'tish + Hududiy xarita
        post("/api/v1/users/register") {
            val req = call.receive<UserRegisterRequest>()
            val now = System.currentTimeMillis()
            var finalId = req.userId.filter { it.isDigit() }
            if (finalId.length != 9) {
                finalId = generate9DigitId()
            }

            DatabaseManager.dataSource?.connection?.use { conn ->
                // Agar tasodifan ID band bo'lsa va boshqa odamga tegishli bo'lsa, yangi 9 xonali ID tanlash
                if (req.userId.isBlank()) {
                    var attempts = 0
                    while (attempts < 10) {
                        var exists = false
                        conn.prepareStatement("SELECT 1 FROM app_users WHERE user_id = ?").use { ps ->
                            ps.setString(1, finalId)
                            ps.executeQuery().use { rs -> exists = rs.next() }
                        }
                        if (!exists) break
                        finalId = generate9DigitId()
                        attempts++
                    }
                }

                conn.prepareStatement(
                    """
                    INSERT INTO app_users(user_id, name, region, district, school, grade, avatar, last_seen_ms)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (user_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        region = EXCLUDED.region,
                        district = EXCLUDED.district,
                        school = EXCLUDED.school,
                        grade = EXCLUDED.grade,
                        avatar = EXCLUDED.avatar,
                        last_seen_ms = EXCLUDED.last_seen_ms
                    """.trimIndent()
                ).use { ps ->
                    ps.setString(1, finalId)
                    ps.setString(2, req.name.trim())
                    ps.setString(3, req.region.trim())
                    ps.setString(4, req.district.trim())
                    ps.setString(5, req.school.trim())
                    ps.setString(6, req.grade.trim())
                    ps.setString(7, req.avatar.trim())
                    ps.setLong(8, now)
                    ps.executeUpdate()
                }
            }
            onlineUsers[finalId] = now
            lastSeenUsers[finalId] = now
            call.respond(
                UserProfileDto(
                    userId = finalId,
                    name = req.name.trim(),
                    region = req.region.trim(),
                    district = req.district.trim(),
                    school = req.school.trim(),
                    grade = req.grade.trim(),
                    avatar = req.avatar.trim(),
                    online = true
                )
            )
        }

        post("/api/v1/users/heartbeat") {
            val req = call.receive<UserHeartbeatRequest>()
            val now = System.currentTimeMillis()
            if (req.userId.isNotBlank()) {
                onlineUsers[req.userId] = now
                lastSeenUsers[req.userId] = now
                DatabaseManager.dataSource?.connection?.use { conn ->
                    if (req.region.isNotBlank()) {
                        conn.prepareStatement("UPDATE app_users SET last_seen_ms = ?, region = ? WHERE user_id = ?").use { ps ->
                            ps.setLong(1, now)
                            ps.setString(2, req.region.trim())
                            ps.setString(3, req.userId)
                            ps.executeUpdate()
                        }
                    } else {
                        conn.prepareStatement("UPDATE app_users SET last_seen_ms = ? WHERE user_id = ?").use { ps ->
                            ps.setLong(1, now)
                            ps.setString(2, req.userId)
                            ps.executeUpdate()
                        }
                    }
                }
            }
            call.respond(HttpStatusCode.OK, mapOf("status" to "ok"))
        }

        get("/api/v1/users/search") {
            val queryId = call.request.queryParameters["id"]?.trim() ?: ""
            if (queryId.isBlank()) {
                call.respond(HttpStatusCode.BadRequest, mapOf("error" to "ID kiritilmadi"))
                return@get
            }
            val ds = DatabaseManager.dataSource
            if (ds == null) {
                call.respond(HttpStatusCode.NotFound, mapOf("error" to "Baza ulanmagan"))
                return@get
            }
            var found: UserProfileDto? = null
            val now = System.currentTimeMillis()
            ds.connection.use { conn ->
                conn.prepareStatement(
                    "SELECT user_id, name, region, district, school, grade, avatar, last_seen_ms FROM app_users WHERE user_id = ? LIMIT 1"
                ).use { ps ->
                    ps.setString(1, queryId)
                    ps.executeQuery().use { rs ->
                        if (rs.next()) {
                            val uid = rs.getString("user_id")
                            val dbSeen = rs.getLong("last_seen_ms")
                            val wsSeen = onlineUsers[uid] ?: 0L
                            val isOnline = (now - maxOf(dbSeen, wsSeen)) < 120_000L
                            found = UserProfileDto(
                                userId = uid,
                                name = rs.getString("name"),
                                region = rs.getString("region"),
                                district = rs.getString("district"),
                                school = rs.getString("school"),
                                grade = rs.getString("grade"),
                                avatar = rs.getString("avatar"),
                                online = isOnline
                            )
                        }
                    }
                }
            }
            if (found != null) {
                call.respond(found!!)
            } else {
                call.respond(HttpStatusCode.NotFound, mapOf("error" to "Foydalanuvchi topilmadi"))
            }
        }

        get("/api/v1/users/map-counts") {
            val ds = DatabaseManager.dataSource
            if (ds == null) {
                call.respond(emptyList<RegionOnlineCountDto>())
                return@get
            }
            val threshold = System.currentTimeMillis() - 120_000L
            val list = mutableListOf<RegionOnlineCountDto>()
            ds.connection.use { conn ->
                conn.prepareStatement(
                    "SELECT region, COUNT(*) AS cnt FROM app_users WHERE last_seen_ms >= ? GROUP BY region"
                ).use { ps ->
                    ps.setLong(1, threshold)
                    ps.executeQuery().use { rs ->
                        while (rs.next()) {
                            list.add(
                                RegionOnlineCountDto(
                                    region = rs.getString("region"),
                                    onlineCount = rs.getInt("cnt")
                                )
                            )
                        }
                    }
                }
            }
            call.respond(list)
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

        // 3. Online / Offline presence tekshiruvi
        get("/api/v1/presence") {
            val userId = call.request.queryParameters["user_id"] ?: ""
            val now = System.currentTimeMillis()
            val lastPing = onlineUsers[userId]
            val isOnline = lastPing != null && (now - lastPing) < 45_000L
            if (!isOnline && lastPing != null) {
                onlineUsers.remove(userId)
            }
            call.respond(
                PresenceResponse(
                    userId = userId,
                    online = isOnline,
                    lastSeenMs = lastSeenUsers[userId] ?: 0L
                )
            )
        }

        // 4. Chat & Guruhlar: xabarlarni yuklash (o'qilganlik holati bilan)
        get("/api/v1/messages") {
            val channelId = call.request.queryParameters["channel_id"] ?: "global_chat"
            val viewerId = call.request.queryParameters["viewer_id"] ?: ""
            val limit = (call.request.queryParameters["limit"]?.toIntOrNull() ?: 30).coerceIn(1, 100)
            val ds = DatabaseManager.dataSource
            if (ds == null) {
                call.respond(emptyList<ChatMessageDto>())
                return@get
            }
            val list = mutableListOf<ChatMessageDto>()
            ds.connection.use { conn ->
                var maxPeerReadId = 0L
                if (viewerId.isNotBlank()) {
                    conn.prepareStatement(
                        "SELECT COALESCE(MAX(last_read_id), 0) FROM channel_reads WHERE channel_id = ? AND user_id <> ?"
                    ).use { ps ->
                        ps.setString(1, channelId)
                        ps.setString(2, viewerId)
                        ps.executeQuery().use { rs ->
                            if (rs.next()) maxPeerReadId = rs.getLong(1)
                        }
                    }
                }
                conn.prepareStatement(
                    "SELECT id, channel_id, sender_name, sender_id, text, created_at FROM chat_messages WHERE channel_id = ? ORDER BY id DESC LIMIT ?"
                ).use { ps ->
                    ps.setString(1, channelId)
                    ps.setInt(2, limit)
                    ps.executeQuery().use { rs ->
                        while (rs.next()) {
                            val msgId = rs.getLong("id")
                            val senderId = rs.getString("sender_id")
                            val isReadByPeer = viewerId.isNotBlank() && senderId == viewerId && msgId <= maxPeerReadId
                            list.add(
                                ChatMessageDto(
                                    id = msgId,
                                    channelId = rs.getString("channel_id"),
                                    senderName = rs.getString("sender_name"),
                                    senderId = senderId,
                                    text = rs.getString("text"),
                                    createdAt = rs.getString("created_at") ?: "",
                                    isRead = isReadByPeer
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
            var createdAtStr = ""
            DatabaseManager.dataSource?.connection?.use { conn ->
                conn.prepareStatement(
                    "INSERT INTO chat_messages(channel_id, sender_name, sender_id, text) VALUES (?, ?, ?, ?) RETURNING id, created_at"
                ).use { ps ->
                    ps.setString(1, req.channelId)
                    ps.setString(2, req.senderName)
                    ps.setString(3, req.senderId)
                    ps.setString(4, req.text.trim())
                    ps.executeQuery().use { rs ->
                        if (rs.next()) {
                            savedId = rs.getLong(1)
                            createdAtStr = rs.getString(2) ?: ""
                        }
                    }
                }
            }
            val dto = ChatMessageDto(
                id = savedId,
                channelId = req.channelId,
                senderName = req.senderName,
                senderId = req.senderId,
                text = req.text.trim(),
                createdAt = createdAtStr,
                isRead = false
            )
            val wsEnvelope = buildJsonObject {
                put("type", "message")
                put("id", dto.id)
                put("channelId", dto.channelId)
                put("senderName", dto.senderName)
                put("senderId", dto.senderId)
                put("text", dto.text)
                put("createdAt", dto.createdAt)
            }.toString()
            broadcastToRoom(req.channelId, wsEnvelope)
            call.respond(HttpStatusCode.Created, dto)
        }

        post("/api/v1/messages/read") {
            val req = call.receive<MarkReadRequest>()
            DatabaseManager.dataSource?.connection?.use { conn ->
                conn.prepareStatement(
                    """
                    INSERT INTO channel_reads(channel_id, user_id, last_read_id, updated_at)
                    VALUES (?, ?, ?, NOW())
                    ON CONFLICT (channel_id, user_id)
                    DO UPDATE SET
                        last_read_id = GREATEST(channel_reads.last_read_id, EXCLUDED.last_read_id),
                        updated_at = NOW()
                    """.trimIndent()
                ).use { ps ->
                    ps.setString(1, req.channelId)
                    ps.setString(2, req.userId)
                    ps.setLong(3, req.lastReadId)
                    ps.executeUpdate()
                }
            }
            val wsEnvelope = buildJsonObject {
                put("type", "read_receipt")
                put("channelId", req.channelId)
                put("userId", req.userId)
                put("lastReadId", req.lastReadId)
            }.toString()
            broadcastToRoom(req.channelId, wsEnvelope)
            call.respond(HttpStatusCode.OK, mapOf("status" to "ok"))
        }

        webSocket("/ws/chat/{channelId}") {
            val channelId = call.parameters["channelId"] ?: "global_chat"
            val userId = call.request.queryParameters["userId"] ?: ""
            val set = rooms.computeIfAbsent(channelId) { ConcurrentHashMap.newKeySet() }
            set.add(this)

            if (userId.isNotBlank()) {
                val now = System.currentTimeMillis()
                onlineUsers[userId] = now
                lastSeenUsers[userId] = now
                val presenceOn = buildJsonObject {
                    put("type", "presence")
                    put("userId", userId)
                    put("online", true)
                    put("lastSeenMs", now)
                }.toString()
                broadcastToRoom(channelId, presenceOn, exclude = this)
            }

            try {
                for (frame in incoming) {
                    if (frame is Frame.Text) {
                        val raw = frame.readText()
                        if (userId.isNotBlank()) {
                            val now = System.currentTimeMillis()
                            onlineUsers[userId] = now
                            lastSeenUsers[userId] = now
                        }
                        val type = runCatching {
                            json.parseToJsonElement(raw).jsonObject["type"]?.jsonPrimitive?.contentOrNull
                        }.getOrNull()

                        if (type != "ping") {
                            broadcastToRoom(channelId, raw, exclude = this)
                        }
                    }
                }
            } finally {
                set.remove(this)
                if (userId.isNotBlank()) {
                    val now = System.currentTimeMillis()
                    onlineUsers.remove(userId)
                    lastSeenUsers[userId] = now
                    val presenceOff = buildJsonObject {
                        put("type", "presence")
                        put("userId", userId)
                        put("online", false)
                        put("lastSeenMs", now)
                    }.toString()
                    broadcastToRoom(channelId, presenceOff, exclude = this)
                }
            }
        }

        // 5. AI Repetitor (FAQ kesh + Fallback zanjiri)
        post("/api/v1/ai/ask") {
            val req = call.receive<AiAskRequest>()
            val norm = req.question.trim().lowercase()
            if (norm.isBlank()) {
                call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Savol bo'sh"))
                return@post
            }

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

            for (modelName in listOf("gemini-3.8-flash", "gemini-3.7-flash")) {
                try {
                    val interBody = buildJsonObject {
                        put("model", modelName)
                        put("input", prompt)
                    }
                    val httpResp = httpClient.post("https://generativelanguage.googleapis.com/v1beta/interactions?key=$geminiApiKey") {
                        contentType(ContentType.Application.Json)
                        setBody(interBody.toString())
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
