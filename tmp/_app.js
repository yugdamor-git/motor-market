import Layout from '../components/layout'
import '../styles/globals.css'
import NextNProgress from 'nextjs-progressbar';
import Script from "next/script";


function MyApp({ Component, pageProps }) {

  return (
    <>

      
      <Script
        id="gtag-script-1"
        strategy="lazyOnload"
        src={`https://www.googletagmanager.com/gtag/js?id=${process.env.NEXT_PUBLIC_GOOGLE_ANALYTICS}`}
      />

      <Script id="gtag-script-2" strategy="lazyOnload">
        {`
            window.dataLayer = window.dataLayer || [];
            function gtag(){dataLayer.push(arguments);}
            gtag('js', new Date());
            gtag('config', '${process.env.NEXT_PUBLIC_GOOGLE_ANALYTICS}', {
              page_path: window.location.pathname,
            });
                `}
      </Script>

    <Layout>
    <NextNProgress
    options={{ easing: 'ease', speed: 500,showSpinner: false }}
    color="#6468fc"
    startPosition={0.3}
    stopDelayMs={200}
    height={3}
    showOnShallow={false}
  />
    <Component {...pageProps} />
    
    </Layout>

    </>
  )
}

export default MyApp
