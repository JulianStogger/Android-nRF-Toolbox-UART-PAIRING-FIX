package no.nordicsemi.android.toolbox.profile.manager

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.withContext
import no.nordicsemi.android.toolbox.profile.manager.repository.UartRepository
import no.nordicsemi.android.toolbox.lib.utils.Profile
import no.nordicsemi.kotlin.ble.client.RemoteCharacteristic
import no.nordicsemi.kotlin.ble.client.RemoteService
import no.nordicsemi.kotlin.ble.core.CharacteristicProperty
import no.nordicsemi.kotlin.ble.core.WriteType
import no.nordicsemi.kotlin.ble.core.util.chunked
import timber.log.Timber
import java.util.UUID
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.toKotlinUuid
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.retryWhen
import no.nordicsemi.kotlin.ble.core.BondState

private val UART_RX_CHARACTERISTIC_UUID: UUID =
    UUID.fromString("6E400002-B5A3-F393-E0A9-E50E24DCCA9E")
private val UART_TX_CHARACTERISTIC_UUID: UUID =
    UUID.fromString("6E400003-B5A3-F393-E0A9-E50E24DCCA9E")

internal class UARTManager : ServiceManager {
    override val profile: Profile
        get() = Profile.UART

    @OptIn(ExperimentalUuidApi::class)
    override suspend fun observeServiceInteractions(
        deviceId: String,
        remoteService: RemoteService,
        scope: CoroutineScope
    ) {
        withContext(scope.coroutineContext) {
            val txCharacteristic = remoteService.characteristics
                .firstOrNull { it.uuid == UART_TX_CHARACTERISTIC_UUID.toKotlinUuid() }

            txCharacteristic?.subscribe()
                ?.retryWhen { cause, attempt ->
                    Timber.tag("UARTManager").w("Subscription failed (attempt $attempt)")
                    
                    if (attempt < 3) {
                        // We use a simple delay here to bypass the 'device' reference issue
                        // while still giving the OS time to bond.
                        delay(2000) 
                        true 
                    } else {
                        false
                    }
                }
                ?.mapNotNull { String(it) }
                ?.onEach { UartRepository.onNewMessageReceived(deviceId, it) }
                ?.catch { e ->
                    Timber.tag("UARTManager").e(e, "NUS TX Subscription failed permanently")
                }
                ?.onCompletion {
                    UartRepository.clear(deviceId)
                }
                ?.launchIn(scope)

            // RX Characteristic (Writing to peripheral)
            val writeCharacteristics = remoteService.characteristics
                .firstOrNull { it.uuid == UART_RX_CHARACTERISTIC_UUID.toKotlinUuid() }
                ?.also { rxCharacteristic = it }

            writeCharacteristics?.properties?.let {
                if (it.contains(CharacteristicProperty.WRITE_WITHOUT_RESPONSE)) {
                    rxCharacteristicWriteType = WriteType.WITHOUT_RESPONSE
                } else if (it.contains(CharacteristicProperty.WRITE)) {
                    rxCharacteristicWriteType = WriteType.WITH_RESPONSE
                }
            }
        }
    }

    companion object {
        private lateinit var rxCharacteristic: RemoteCharacteristic
        private var rxCharacteristicWriteType: WriteType? = null

        suspend fun sendText(
            device: String,
            message: String,
            maxWriteLength: Int,
        ) {
            val messageBytes = message.toByteArray()
            try {
                if (rxCharacteristicWriteType == null) {
                    Timber.e("Write type not set.")
                    // Todo: Handle this case.
                } else {
                    messageBytes.chunked(maxWriteLength).forEach {
                        rxCharacteristic.write(it, rxCharacteristicWriteType!!)
                    }
                }
            } catch (e: Exception) {
                Timber.tag("UARTService").e("Error ${e.message}")
            } finally {
                UartRepository.onNewMessageSent(device, message)
            }
        }
    }
}