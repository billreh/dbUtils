package net.tralfamadore.dbUtils;

import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import javax.persistence.Entity;
import javax.persistence.MappedSuperclass;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Class: Bootstrap
 * Created by billreh on 9/3/17.
 */
public class Bootstrap {
    private static SessionFactory sessionFactory;

    private static void bootstrap() {
        if(sessionFactory != null)
            return;
        synchronized (sessionFactory) {
            System.out.printf("moo");
            Configuration cfg = new Configuration();
            cfg.setProperty("hibernate.connection.username", "root");
            cfg.setProperty("hibernate.connection.password", "root");
            cfg.setProperty("hibernate.connection.url", "jdbc:mysql://localhost:3306/test");
            hibernateAnnotatedClasses().forEach(cfg::addAnnotatedClass);
            sessionFactory = cfg.buildSessionFactory(
                    new StandardServiceRegistryBuilder()
                            .applySettings(cfg.getProperties())
                            .build());
        }
    }

    private static synchronized List<Class<? extends Object>> hibernateAnnotatedClasses(){
        System.out.printf("moo");
        List<Class <? extends Object>> hibernateAnnotatedClasses = new ArrayList<>();

        List<ClassLoader> classLoadersList = new LinkedList<>();
        classLoadersList.add(ClasspathHelper.contextClassLoader());
        classLoadersList.add(ClasspathHelper.staticClassLoader());
        classLoadersList.add(Bootstrap.class.getClassLoader());


        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setScanners(new SubTypesScanner(false /* don't exclude Object.class */), new ResourcesScanner())
                .setUrls(ClasspathHelper.forClassLoader(classLoadersList.toArray(new ClassLoader[0])))
                // TODO if we need to go down to the beans folder we have to figure out how to get the project name,
                .filterInputsBy(new FilterBuilder().include(FilterBuilder.prefix("net.tralfamadore"))));

        Set<Class<? extends Object>> allClasses =
                reflections.getSubTypesOf(Object.class);

        for(Class<?> clazz : allClasses) {
            if (clazz.isAnnotationPresent(Entity.class) || clazz.isAnnotationPresent(MappedSuperclass.class)){
                hibernateAnnotatedClasses.add(clazz);
            }
        }

        return hibernateAnnotatedClasses;
    }

    public static void main(String[] args) {
        bootstrap();
        sessionFactory.openSession().createNativeQuery("select * from address").getResultList().forEach(System.out::println);
        sessionFactory.close();
    }
}
