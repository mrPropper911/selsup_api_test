package by.belyahovich;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;

public class CrptApi {

    private static final String URL_API = "https://ismp.crpt.ru";

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    private static final Logger log = LogManager.getLogger(CrptApi.class);
    private static volatile ScheduledFuture<?> task;
    private static int currentNumberOfConnection = 0;
    private static String responseValue = null;
    private final TimeUnit timeUnit;
    private final int requestLimit;
    private Authentication authentication;

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        if (requestLimit < 0) {
            log.error("Request limit should be positive");
            throw new IllegalArgumentException("Request limit should be positive");
        }
        this.timeUnit = timeUnit;
        this.requestLimit = requestLimit;
        this.authentication = new Authentication();
    }

    public static void main(String[] args) throws InterruptedException {

        CrptApi crptApi = new CrptApi(TimeUnit.SECONDS, 5);//todo
        TimeUnit timeUnit1 = crptApi.getTimeUnit();
        long l = timeUnit1.toSeconds(1);

        Document document = Document.builder()
                .productGroup(ProductGroup.mapOfProductGroup.get(8))
                .productDocument(ProductDocument.builder().dateFrom("Test").build())
                .documentFormat(DocumentFormat.MANUAL)
                .type(DocumentType.LK_CONTRACT_COMMISSIONING)
                .build();

        Signature signature = new Signature();

        crptApi.initRepeatableTask(document, signature);


    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public int getRequestLimit() {
        return requestLimit;
    }

    public void initRepeatableTask(Document document, Signature signature) {
        task = executorService.scheduleWithFixedDelay(() -> {
            ++currentNumberOfConnection;
            responseValue = putIntoCirculationGoodsProducedOnRussia(document, signature);
            if (responseValue != null) {
                currentNumberOfConnection = getRequestLimit();
            }
        }, 0, getThreadSleepOnSecond(), getTimeUnit());


        while (true) {
            Thread.onSpinWait();
            if (currentNumberOfConnection == getRequestLimit()) {
                task.cancel(true);
                executorService.shutdown();
                break;
            }
        }

        System.out.println(responseValue + " --");
        int i = 0;
    }


    private int getThreadSleepOnSecond() {
        long timeDuration = 0;
        //todo comment
        if (this.timeUnit == TimeUnit.SECONDS) {
            timeDuration = this.timeUnit.toSeconds(30);
        } else {
            timeDuration = this.timeUnit.toSeconds(1);
        }
        return (int) (timeDuration / this.requestLimit);

    }

    private String putIntoCirculationGoodsProducedOnRussia(Document document, Signature signature) {
        if (document.getType() != DocumentType.LK_CONTRACT_COMMISSIONING &&
                document.getType() != DocumentType.LK_CONTRACT_COMMISSIONING_CSV &&
                document.getType() != DocumentType.LK_CONTRACT_COMMISSIONING_XML) {
            log.error("Ilegual document type, provide type: LK_CONTRACT_COMMISSIONING...");
            throw new IllegalArgumentException("Ilegual document type");
        }

        try {
            String jsonProductDocument = objectMapper.writeValueAsString(document.getProductDocument());
            document.setProductDocument(null);//clear for security data
            String jsonProductDocumentBase64 = Base64.getEncoder().encodeToString(jsonProductDocument.getBytes());

            JsonNode jsonNodeDocument = objectMapper.valueToTree(document);
            ((ObjectNode) jsonNodeDocument).put("signature", Base64.getEncoder().encode(signature.getElectronSignature().getBytes()));
            ((ObjectNode) jsonNodeDocument).put("product_document", jsonProductDocumentBase64);
            String jsonBody = objectMapper.writeValueAsString(jsonNodeDocument);

            authentication = Authentication.getAuthenticationToken(authentication);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(URL_API + "/api/v3/lk/documents/create?pg=" + document.getProductGroup()))
                    .setHeader("Content-Type", "application/json")
                    .setHeader("Authorization", authentication.getToken())
                    .POST(HttpRequest.BodyPublishers.ofString(Objects.requireNonNull(jsonBody)))
                    .build();

            HttpClient httpClient = HttpClient.newBuilder()
                    .version(HttpClient.Version.HTTP_1_1)
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                JsonNode jsonNode = objectMapper.readTree(response.body());
                log.info("Document ");
                return jsonNode.get("value").asText();
            } else {
                log.info("Status code: " + response.statusCode() + " {" + response.body() + "}");
                return null;
            }
        } catch (Exception e) {
            log.error("An error was received while saving the document: " + e.getMessage());
            throw new RuntimeException("An error was received while saving the document");
        }
    }

    enum DocumentType {
        AGGREGATION_DOCUMENT, AGGREGATION_DOCUMENT_CSV, AGGREGATION_DOCUMENT_XML,
        DISAGGREGATION_DOCUMENT, DISAGGREGATION_DOCUMENT_CSV, DISAGGREGATION_DOCUMENT_XML,
        REAGGREGATION_DOCUMENT, REAGGREGATION_DOCUMENT_CSV, REAGGREGATION_DOCUMENT_XML,
        LP_INTRODUCE_GOODS, LP_INTRODUCE_GOODS_CSV, LP_INTRODUCE_GOODS_XML,
        LP_SHIP_GOODS, LP_SHIP_GOODS_CSV, LP_SHIP_GOODS_XML,
        LK_REMARK, LK_REMARK_CSV, LK_REMARK_XML,
        LK_RECEIPT, LK_RECEIPT_CSV, LK_RECEIPT_XML,
        LP_GOODS_IMPORT, LP_GOODS_IMPORT_CSV, LP_GOODS_IMPORT_XML,
        LP_CANCEL_SHIPMENT, LP_CANCEL_SHIPMENT_CSV, LP_CANCEL_SHIPMENT_XML,
        LK_KM_CANCELLATION, LK_KM_CANCELLATION_CSV, LK_KM_CANCELLATION_XML,
        LK_APPLIED_KM_CANCELLATION, LK_APPLIED_KM_CANCELLATION_CSV, LK_APPLIED_KM_CANCELLATION_XML,
        LK_CONTRACT_COMMISSIONING, LK_CONTRACT_COMMISSIONING_CSV, LK_CONTRACT_COMMISSIONING_XML,
        LK_INDI_COMMISSIONING, LK_INDI_COMMISSIONING_CSV, LK_INDI_COMMISSIONING_XML,
        LP_SHIP_RECEIPT, LP_SHIP_RECEIPT_CSV, LP_SHIP_RECEIPT_XML,
        OST_DESCRIPTION, OST_DESCRIPTION_CSV, OST_DESCRIPTION_XML,
        CROSSBORDER, CROSSBORDER_CSV, CROSSBORDER_XML,
        LP_INTRODUCE_OST, LP_INTRODUCE_OST_CSV, LP_INTRODUCE_OST_XML,
        LP_RETURN, LP_RETURN_CSV, LP_RETURN_XML,
        LP_SHIP_GOODS_CROSSBORDER, LP_SHIP_GOODS_CROSSBORDER_CSV, LP_SHIP_GOODS_CROSSBORDER_XML,
        LP_CANCEL_SHIPMENT_CROSSBORDER
    }

    enum DocumentFormat {
        MANUAL, XML, CSV
    }

    enum DocumentStatus {
        IN_PROGRESS, CHECKED_OK, CHECKED_NOT_OK, PROCESSING_ERROR, CANCELLED, ACCEPTED,
        WAIT_ACCEPTANCE, WAIT_PARTICIPANT_REGISTRATION
    }

    enum Order {
        ASC, DESC
    }

    enum PageDir {
        PREV, NEXT
    }

    @Data
    @FieldNameConstants
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Authentication {
        //API v16.5 Token lifetime - 10 hours
        private static final long TOKEN_LIFETIME_IN_SECONDS = 60 * 60 * 10;
        private static final String TEST_TOKEN = "TEST_TOKEN";

        private String uuid;
        private String data;
        private String token;
        private LocalTime localTime;

        /**
         * <p> Return unique identifier of the generated random data (uuid) and random string of data (data)
         * <p> Example GET request <a href="https://ismp.crpt.ru/api/v3/auth/cert/key">https://ismp.crpt.ru/api/v3/auth/cert/key</a>
         *
         * @return {@link Authentication} with {@link String} uuid and {@link String} data
         */
        private static Authentication authorizationRequest() {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create("https://ismp.crpt.ru/api/v3/auth/cert/key"))
                        .GET()
                        .build();

                HttpClient httpClient = HttpClient.newBuilder()
                        .version(HttpClient.Version.HTTP_1_1)
                        .connectTimeout(Duration.ofSeconds(10))
                        .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                log.info("Successfully received uuid and data: " + response.body());
                return objectMapper.readValue(response.body(), Authentication.class);
            } catch (IOException | InterruptedException e) {
                log.error("Return error when trying to get uuid and data " + e.getMessage());
                throw new IllegalArgumentException("Return error when trying to get uuid and data ");
            }
        }

        /**
         * <p> Return authentication with token
         * <p> Example POST request <a href="https://ismp.crpt.ru/api/v3/auth/cert/">https://ismp.crpt.ru/api/v3/auth/cert/</a>
         *
         * @return {@link Authentication} with {@link String} token
         * <p> { {@code @//}  FIXME : 11/19/2023 need to add a real signature
         */
        public static Authentication getAuthenticationToken(Authentication authentication) {
            if (authentication.getToken() != null && authentication.getLocalTime() != null
                    && ChronoUnit.SECONDS.between(authentication.getLocalTime(), LocalTime.now()) < TOKEN_LIFETIME_IN_SECONDS) {
                log.info("Return existing working token: " + authentication.getToken());
                return authentication;
            }
            try {
                authentication = authorizationRequest();
                authentication.setData(Base64.getEncoder()
                        .encodeToString((authentication.getData() + new Signature().getElectronSignature()).getBytes()));

                String jsonBody = objectMapper.writeValueAsString(authentication);

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create("https://ismp.crpt.ru/api/v3/auth/cert/"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(Objects.requireNonNull(jsonBody)))
                        .build();


                HttpClient httpClient = HttpClient.newBuilder()
                        .version(HttpClient.Version.HTTP_1_1)
                        .connectTimeout(Duration.ofSeconds(10))
                        .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

                //todo for test we need add fake token delete if u have real signature
                if (response.statusCode() == 400 && response.body().contains("Ошибка при проверке подписи")) {
                    log.info("Return status code 400, for test add TEST_TOKEN");
                    authentication.setLocalTime(LocalTime.now());
                    authentication.setToken("Bearer " + TEST_TOKEN);
                    return authentication;
                }

                Authentication authWithToken = objectMapper.readValue(response.body(), Authentication.class);
                authentication.setLocalTime(LocalTime.now());
                authentication.setToken("Bearer " + authWithToken.getToken());
                return authentication;
            } catch (IOException | InterruptedException e) {
                log.error("Return error when trying get token" + e.getMessage());
                throw new RuntimeException(e);
            }
        }
    }

    @Data
    @Builder
    public static class Document {

        @JsonProperty("document_format")
        private DocumentFormat documentFormat;

        @JsonProperty("product_document")
        private ProductDocument productDocument;

        @JsonProperty("product_group")
        private String productGroup;

        private DocumentType type;
    }

    @Data
    @Builder
    public static class ProductDocument {
        private String dateFrom;
        private String dateTo;
        private String did;
        private DocumentFormat documentFormat;
        private DocumentStatus documentStatus;
        private DocumentType documentType;
        private boolean inputFormat;
        private int limit;
        private String number;
        private Order order;
        private String orderColumn;
        private PageDir pageDir;
        private String participantInn;
        private List<ProductGroup> pg;
    }

    //todo Need electronic signature and a personal account for real impl
    public static class Signature {

        private static final String TEST_SIGNATURE = "TEST_SIGNATURE";

        @JsonProperty("signature")
        private String electronSignature;

        public Signature() {
            this.electronSignature = TEST_SIGNATURE;
        }

        public String getElectronSignature() {
            return electronSignature;
        }
    }

    @Data
    @NoArgsConstructor
    public static class ProductGroup {

        private static Map<Integer, String> mapOfProductGroup = new ConcurrentHashMap<>();

        static {
            mapOfProductGroup.put(1, "clothes");
            mapOfProductGroup.put(2, "shoes");
            mapOfProductGroup.put(3, "tobacco");
            mapOfProductGroup.put(4, "perfumery");
            mapOfProductGroup.put(5, "tires");
            mapOfProductGroup.put(6, "electronics");
            mapOfProductGroup.put(7, "pharma");
            mapOfProductGroup.put(8, "milk");
            mapOfProductGroup.put(9, "bicycle");
            mapOfProductGroup.put(10, "wheelchairs");
        }
    }
}