/**
 * libdecsync - Utils.kt
 *
 * Copyright (C) 2019 Aldo Gunsing
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, see <http://www.gnu.org/licenses/>.
 */

package org.decsync.library

import android.os.Build
import android.os.Environment
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.*

actual fun getDeviceName(): String = Build.MODEL

actual fun currentDatetime(): String = iso8601Format.format(Date())
private val iso8601Format: DateFormat =
        SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").apply {
            timeZone = TimeZone.getTimeZone("UTC")
        }

actual fun getDefaultDecsyncDir(): String =
        "${Environment.getExternalStorageDirectory()}/DecSync"

@ExperimentalStdlibApi
actual fun byteArrayToString(input: ByteArray): String = input.decodeToString()