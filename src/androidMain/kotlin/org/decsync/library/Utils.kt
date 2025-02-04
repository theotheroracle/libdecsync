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

import android.os.AsyncTask
import android.os.Build
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.*

actual fun getDeviceName(): String = Build.MODEL

actual fun currentDatetime(): String = iso8601Format.format(Date())
actual fun oldDatetime(): String = iso8601Format.format(Date().time - 1000L*60*60*24*30)
private val iso8601Format: DateFormat =
        SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").apply {
            timeZone = TimeZone.getTimeZone("UTC")
        }

@ExperimentalStdlibApi
actual fun byteArrayToString(input: ByteArray): String = input.decodeToString()

actual fun async(f: () -> Unit) = AsyncTask.execute(f)